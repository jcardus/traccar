package org.traccar.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.traccar.ProtocolTest;
import org.traccar.model.Command;

public class HuabaoProtocolEncoderTest extends ProtocolTest {

    @Test
    public void testEncodeVideoRequestData() {
        var data = Unpooled.buffer();
        HuabaoProtocolEncoder.encodeVideoRequestData(data, "media-ip.n-gps.com", 10002, 1);
        assertEquals(
                "126d656469612d69702e6e2d6770732e636f6d27122712010000",
                ByteBufUtil.hexDump(data));
        data.release();
    }

    @Disabled
    @Test
    public void testEncode() throws Exception {

        var encoder = inject(new HuabaoProtocolEncoder(null));

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_ENGINE_STOP);

        verifyCommand(encoder, command, binary("7e81050001080201000027001ff0467e"));

    }

}
