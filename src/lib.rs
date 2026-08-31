use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use napi::{
    bindgen_prelude::{Buffer, Result},
    threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode},
    tokio::{
        self,
        io::{AsyncReadExt, AsyncWriteExt},
        sync::{mpsc, oneshot},
    },
    Error, Status,
};
use napi_derive::napi;
use tokio_serial::{SerialPort, SerialPortBuilderExt, SerialPortType};

fn napi_error(message: impl Into<String>) -> Error {
    Error::new(Status::GenericFailure, message.into())
}

enum Command {
    Write(Vec<u8>, oneshot::Sender<std::io::Result<()>>),
    RequestToSend(bool, oneshot::Sender<tokio_serial::Result<()>>),
    DataTerminalReady(bool, oneshot::Sender<tokio_serial::Result<()>>),
}

#[napi(object)]
pub struct SerialPortOptions {
    /// Defaults to 115_200.
    pub baud_rate: Option<u32>,
    /// Set the number of bits used to represent a character sent on the line.
    ///
    /// Defaults to 8.
    #[napi(ts_type = "5 | 6 | 7 | 8")]
    pub data_bits: Option<u8>,
    /// Set the type of parity to use for error checking.
    ///
    /// Defaults to "none".
    #[napi(ts_type = "\"none\" | \"odd\" | \"even\"")]
    pub parity: Option<String>,
    /// Set the number of bits to use to signal the end of a character.
    ///
    /// Defaults to 1.
    #[napi(ts_type = "1 | 2")]
    pub stop_bits: Option<u8>,
    /// Set the type of signalling to use for controlling data transfer.
    ///
    /// Defaults to "none".
    #[napi(ts_type = "\"none\" | \"software\" | \"hardware\"")]
    pub flow_control: Option<String>,
    /// Set data terminal ready (DTR) to the given state when opening the device.
    ///
    /// If `None`, preserve the state of data terminal ready (DTR) when opening the device.
    pub dtr_on_open: Option<bool>,
}

#[napi]
pub struct NativeSerialPort {
    closed: Arc<AtomicBool>,
    command_tx: mpsc::Sender<Command>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

#[napi]
impl NativeSerialPort {
    #[napi(factory)]
    pub async fn open(
        path: String,
        options: Option<SerialPortOptions>,
        callback: ThreadsafeFunction<Buffer, ()>,
        channel_capacity: u16,
    ) -> Result<Self> {
        let config = serial_config(options)?;
        let mut builder = tokio_serial::new(path, config.baud_rate)
            .data_bits(config.data_bits)
            .parity(config.parity)
            .stop_bits(config.stop_bits)
            .flow_control(config.flow_control);

        if let Some(dtr_on_open) = config.dtr_on_open {
            builder = builder.dtr_on_open(dtr_on_open);
        } else {
            builder = builder.preserve_dtr_on_open();
        }

        // requires being in the async runtime, hence the async factory fn
        let mut stream = builder
            .open_native_async()
            .map_err(|error| napi_error(format!("failed to open serial port: {error}")))?;
        let (command_tx, mut command_rx) = mpsc::channel(channel_capacity as usize);
        let closed = Arc::new(AtomicBool::new(false));

        let task = tokio::spawn({
            let closed_clone = Arc::clone(&closed);

            async move {
                let mut buffer = vec![0_u8; 4096];

                loop {
                    tokio::select! {
                        command = command_rx.recv() => {
                            match command {
                                Some(Command::Write(data, reply)) => {
                                    let result = async {
                                        stream.write_all(&data).await?;
                                        stream.flush().await
                                    }
                                    .await;

                                    let _ = reply.send(result);
                                }

                                Some(Command::RequestToSend(level, reply)) => {
                                    let _ = reply.send(stream.write_request_to_send(level));
                                }

                                Some(Command::DataTerminalReady(level, reply)) => {
                                    let _ = reply.send(stream.write_data_terminal_ready(level));
                                }

                                None => break,
                            }
                        }

                        result = stream.read(&mut buffer) => {
                            match result {
                                Ok(0) => break,

                                Ok(length) => {
                                    let data = Buffer::from(buffer[..length].to_vec());
                                    let _ = callback.call(
                                        Ok(data),
                                        ThreadsafeFunctionCallMode::NonBlocking,
                                    );
                                }

                                Err(error) => {
                                    if !closed_clone.load(Ordering::Acquire) {
                                        let _ = callback.call(
                                            Err(napi_error(format!("serial read error: {error}"))),
                                            ThreadsafeFunctionCallMode::NonBlocking,
                                        );
                                    }

                                    break;
                                }
                            }
                        }
                    }

                    if closed_clone.load(Ordering::Acquire) {
                        break;
                    }
                }
            }
        });

        Ok(Self {
            closed,
            command_tx,
            task: Mutex::new(Some(task)),
        })
    }

    /// Write all bytes to the serial port.
    #[napi]
    pub async fn write(&self, data: Buffer) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::new(Status::Closing, "serial port is closed"));
        }

        let (reply_tx, reply_rx) = oneshot::channel();

        self.command_tx
            .send(Command::Write(data.to_vec(), reply_tx))
            .await
            .map_err(|_| Error::new(Status::Closing, "serial port is closed"))?;

        reply_rx
            .await
            .map_err(|_| Error::new(Status::Closing, "serial port is closed"))?
            .map_err(|error| napi_error(format!("serial write error: {error}")))?;

        Ok(())
    }

    /// Wrap [`SerialPort::write_request_to_send`].
    #[napi]
    pub async fn write_request_to_send(&self, level: bool) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.command_tx
            .send(Command::RequestToSend(level, reply_tx))
            .await
            .map_err(|_| Error::new(Status::Closing, "serial port is closed"))?;

        reply_rx
            .await
            .map_err(|_| Error::new(Status::Closing, "serial port is closed"))?
            .map_err(|error| napi_error(format!("serial write request to send error: {error}")))?;

        Ok(())
    }

    /// Wrap [`SerialPort::write_data_terminal_ready`].
    #[napi]
    pub async fn write_data_terminal_ready(&self, level: bool) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.command_tx
            .send(Command::DataTerminalReady(level, reply_tx))
            .await
            .map_err(|_| Error::new(Status::Closing, "serial port is closed"))?;

        reply_rx
            .await
            .map_err(|_| Error::new(Status::Closing, "serial port is closed"))?
            .map_err(|error| {
                napi_error(format!("serial write data terminal ready error: {error}"))
            })?;

        Ok(())
    }

    /// Abort the command/read task and mark closed.
    #[napi]
    pub async fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let task = self
            .task
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "failed to lock task"))?
            .take();

        if let Some(task) = task {
            task.abort();

            let _ = task.await;
        }

        Ok(())
    }
}

impl Drop for NativeSerialPort {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);

        if let Ok(mut task) = self.task.lock() {
            if let Some(task) = task.take() {
                task.abort();
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SerialConfig {
    baud_rate: u32,
    data_bits: tokio_serial::DataBits,
    parity: tokio_serial::Parity,
    stop_bits: tokio_serial::StopBits,
    flow_control: tokio_serial::FlowControl,
    dtr_on_open: Option<bool>,
}

fn serial_config(options: Option<SerialPortOptions>) -> Result<SerialConfig> {
    let options = options.unwrap_or(SerialPortOptions {
        baud_rate: None,
        data_bits: None,
        parity: None,
        stop_bits: None,
        flow_control: None,
        dtr_on_open: None,
    });
    let data_bits = match options.data_bits.unwrap_or(8) {
        5 => tokio_serial::DataBits::Five,
        6 => tokio_serial::DataBits::Six,
        7 => tokio_serial::DataBits::Seven,
        8 => tokio_serial::DataBits::Eight,
        value => {
            return Err(Error::new(
                Status::InvalidArg,
                format!("invalid data_bits: {value}; expected 5, 6, 7, or 8"),
            ))
        }
    };
    let parity = match options
        .parity
        .as_deref()
        .unwrap_or("none")
        .to_ascii_lowercase()
        .as_str()
    {
        "none" => tokio_serial::Parity::None,
        "odd" => tokio_serial::Parity::Odd,
        "even" => tokio_serial::Parity::Even,
        value => {
            return Err(Error::new(
                Status::InvalidArg,
                format!("invalid parity: {value}; expected none, odd, or even"),
            ))
        }
    };
    let stop_bits = match options.stop_bits.unwrap_or(1) {
        1 => tokio_serial::StopBits::One,
        2 => tokio_serial::StopBits::Two,
        value => {
            return Err(Error::new(
                Status::InvalidArg,
                format!("invalid stop_bits: {value}; expected 1 or 2"),
            ))
        }
    };
    let flow_control = match options
        .flow_control
        .as_deref()
        .unwrap_or("none")
        .to_ascii_lowercase()
        .as_str()
    {
        "none" => tokio_serial::FlowControl::None,
        "software" => tokio_serial::FlowControl::Software,
        "hardware" => tokio_serial::FlowControl::Hardware,
        value => {
            return Err(Error::new(
                Status::InvalidArg,
                format!("invalid flow_control: {value}; expected none, software or hardware"),
            ))
        }
    };

    Ok(SerialConfig {
        baud_rate: options.baud_rate.unwrap_or(115_200),
        data_bits,
        parity,
        stop_bits,
        flow_control,
        dtr_on_open: options.dtr_on_open,
    })
}

#[napi(object)]
pub struct AvailablePort {
    pub port_name: String,
    pub port_type: String,

    pub usb_vid: Option<u16>,
    pub usb_pid: Option<u16>,
    pub serial_number: Option<String>,
    pub manufacturer: Option<String>,
    pub product: Option<String>,
}

#[napi]
pub fn available_ports() -> Result<Vec<AvailablePort>> {
    tokio_serial::available_ports()
        .map(|ports| {
            ports
                .into_iter()
                .map(|port| match port.port_type {
                    SerialPortType::UsbPort(info) => AvailablePort {
                        port_name: port.port_name,
                        port_type: "usb".into(),
                        usb_vid: Some(info.vid),
                        usb_pid: Some(info.pid),
                        serial_number: info.serial_number,
                        manufacturer: info.manufacturer,
                        product: info.product,
                    },

                    SerialPortType::PciPort => AvailablePort {
                        port_name: port.port_name,
                        port_type: "pci".into(),
                        usb_vid: None,
                        usb_pid: None,
                        serial_number: None,
                        manufacturer: None,
                        product: None,
                    },

                    SerialPortType::BluetoothPort => AvailablePort {
                        port_name: port.port_name,
                        port_type: "bluetooth".into(),
                        usb_vid: None,
                        usb_pid: None,
                        serial_number: None,
                        manufacturer: None,
                        product: None,
                    },

                    SerialPortType::Unknown => AvailablePort {
                        port_name: port.port_name,
                        port_type: "unknown".into(),
                        usb_vid: None,
                        usb_pid: None,
                        serial_number: None,
                        manufacturer: None,
                        product: None,
                    },
                })
                .collect()
        })
        .map_err(|error| {
            Error::new(
                Status::GenericFailure,
                format!("failed to enumerate serial ports: {error}"),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_options() -> SerialPortOptions {
        SerialPortOptions {
            baud_rate: None,
            data_bits: None,
            parity: None,
            stop_bits: None,
            flow_control: None,
            dtr_on_open: None,
        }
    }

    #[test]
    fn applies_defaults() {
        let config = serial_config(Some(empty_options())).unwrap();

        assert_eq!(config.baud_rate, 115_200);
        assert_eq!(config.data_bits, tokio_serial::DataBits::Eight);
        assert_eq!(config.parity, tokio_serial::Parity::None);
        assert_eq!(config.stop_bits, tokio_serial::StopBits::One);
        assert_eq!(config.flow_control, tokio_serial::FlowControl::None);
        assert_eq!(config.dtr_on_open, None);
    }

    #[test]
    fn accepts_supported_options() {
        let config = serial_config(Some(SerialPortOptions {
            baud_rate: Some(460_800),
            data_bits: Some(7),
            parity: Some("even".into()),
            stop_bits: Some(2),
            flow_control: Some("hardware".into()),
            dtr_on_open: Some(true),
        }))
        .unwrap();

        assert_eq!(config.baud_rate, 460_800);
        assert_eq!(config.data_bits, tokio_serial::DataBits::Seven);
        assert_eq!(config.parity, tokio_serial::Parity::Even);
        assert_eq!(config.stop_bits, tokio_serial::StopBits::Two);
        assert_eq!(config.flow_control, tokio_serial::FlowControl::Hardware);
        assert_eq!(config.dtr_on_open, Some(true));
    }

    #[test]
    fn rejects_invalid_data_bits() {
        let result = serial_config(Some(SerialPortOptions {
            data_bits: Some(9),
            ..empty_options()
        }));

        assert!(result.is_err());
    }

    #[test]
    fn rejects_invalid_parity() {
        let result = serial_config(Some(SerialPortOptions {
            parity: Some("mark".into()),
            ..empty_options()
        }));

        assert!(result.is_err());
    }

    #[test]
    fn rejects_invalid_stop_bits() {
        let result = serial_config(Some(SerialPortOptions {
            stop_bits: Some(3),
            ..empty_options()
        }));

        assert!(result.is_err());
    }

    #[test]
    fn rejects_invalid_flow_control() {
        let result = serial_config(Some(SerialPortOptions {
            flow_control: Some("bad".into()),
            ..empty_options()
        }));

        assert!(result.is_err());
    }
}
