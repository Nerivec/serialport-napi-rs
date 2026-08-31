import { test, type TestContext } from 'node:test'
import { availablePorts, NativeSerialPort } from '../index.js'

const noop = () => {}

test('rejects an invalid data_bits value', (t: TestContext) => {
    t.assert.rejects(async () => {
        await NativeSerialPort.open(
            '/path/that/does/not/matter',
            {
                // @ts-expect-error bad on purpose
                dataBits: 9,
            },
            noop,
            16,
        )
    }, /data_bits/)
})

test('rejects an invalid parity value', (t: TestContext) => {
    t.assert.rejects(async () => {
        await NativeSerialPort.open(
            '/path/that/does/not/matter',
            {
                // @ts-expect-error bad on purpose
                parity: 'mark',
            },
            noop,
            16,
        )
    }, /parity/)
})

test('rejects an invalid stop_bits value', (t: TestContext) => {
    t.assert.rejects(async () => {
        await NativeSerialPort.open(
            '/path/that/does/not/matter',
            {
                // @ts-expect-error bad on purpose
                stopBits: 3,
            },
            noop,
            16,
        )
    }, /stop_bits/)
})

test('rejects an invalid flow_control value', (t: TestContext) => {
    t.assert.rejects(async () => {
        await NativeSerialPort.open(
            '/path/that/does/not/matter',
            {
                // @ts-expect-error bad on purpose
                flowControl: 'bad',
            },
            noop,
            16,
        )
    }, /flow_control/)
})

test('reports an unavailable device path', (t: TestContext) => {
    t.assert.rejects(async () => {
        await NativeSerialPort.open('/definitely/not/a/real/serial/device', {}, noop, 16)
    })
})

test('availablePorts returns an array', (t: TestContext) => {
    const ports = availablePorts()

    t.assert.ok(Array.isArray(ports))

    for (const port of ports) {
        t.assert.strictEqual(typeof port.portName, 'string')
        t.assert.strictEqual(typeof port.portType, 'string')
    }
})

const testWithDevice = process.env.SERIAL_TEST_DEVICE ? test : test.skip

testWithDevice('opens and closes the configured serial device', async (t: TestContext) => {
    function onData(_data: Buffer) {}

    function onError(_error: Error) {}

    const port = await NativeSerialPort.open(
        process.env.SERIAL_TEST_DEVICE!,
        {
            baudRate: 115200,
            dataBits: 8,
            parity: 'none',
            stopBits: 1,
            flowControl: 'none',
        },
        (error, data) => {
            if (error !== null) {
                onError(error)
            } else {
                onData(data)
            }
        },
        16,
    )

    t.assert.doesNotThrow(() => port.close())
    t.assert.doesNotThrow(() => port.close())
})

testWithDevice('receives bytes from the configured serial device', async (t: TestContext) => {
    let resolveReceived: (value: Buffer) => void
    let rejectReceived: (error: Error) => void

    const received = new Promise<Buffer>((resolve, reject) => {
        resolveReceived = resolve
        rejectReceived = reject
    })

    function onData(data: Buffer) {
        resolveReceived(data)
    }

    function onError(error: Error) {
        rejectReceived(error)
    }

    const port = await NativeSerialPort.open(
        process.env.SERIAL_TEST_DEVICE!,
        {
            baudRate: 115200,
        },
        (error, data) => {
            if (error !== null) {
                onError(error)
            } else {
                onData(data)
            }
        },
        16,
    )

    await port.write(Buffer.from('ping'))

    const data = await received

    port.close()

    t.assert.strictEqual(data, Buffer.from('ping'))
})
