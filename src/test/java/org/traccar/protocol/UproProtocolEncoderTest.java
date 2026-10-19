package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.traccar.ProtocolTest;
import org.traccar.model.Command;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UproProtocolEncoderTest extends ProtocolTest {

    @Test
    public void testEncode() throws Exception {

        var encoder = inject(new UproProtocolEncoder(null));

        Command command = new Command();
        command.setDeviceId(1);

        command.setType(Command.TYPE_ENGINE_STOP);
        assertEquals("*HQ2011BB1#", encoder.encodeCommand(command));

        command.setType(Command.TYPE_ENGINE_RESUME);
        assertEquals("*HQ2011BB0#", encoder.encodeCommand(command));

        command.setType(Command.TYPE_CUSTOM);
        command.set(Command.KEY_DATA, "*HQ2011BB1#");
        assertEquals("*HQ2011BB1#", encoder.encodeCommand(command));

    }

}
