package org.traccar.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.traccar.ProtocolTest;
import org.traccar.model.Command;

import java.util.TimeZone;

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

    @Test
    public void testEncodeVideoListNoAlarmFilter() {
        ByteBuf data = Unpooled.buffer();
        try {
            HuabaoProtocolEncoder.encodeVideoListData(
                    data, 0, "2026-08-31T09:29:16.000Z", "2026-08-31T09:29:36.000Z", 0,
                    TimeZone.getTimeZone("UTC"));
            assertEquals("002608310929162608310929360000000000000000020000", ByteBufUtil.hexDump(data));
        } finally {
            data.release();
        }
    }

    @Test
    public void testEncodeVideoListWithAlarmFilter() {
        ByteBuf data = Unpooled.buffer();
        try {
            HuabaoProtocolEncoder.encodeVideoListData(
                    data, 3, "2026-08-31T09:29:16.000Z", "2026-08-31T09:29:36.000Z", 0x100000000L,
                    TimeZone.getTimeZone("UTC"));
            assertEquals("032608310929162608310929360000000100000000020000", ByteBufUtil.hexDump(data));
        } finally {
            data.release();
        }
    }

    @Test
    public void testEncodeVideoListDeviceTimeZone() {
        ByteBuf data = Unpooled.buffer();
        try {
            HuabaoProtocolEncoder.encodeVideoListData(
                    data, 0, "2026-08-31T09:29:16.000Z", "2026-08-31T09:29:36.000Z", 0,
                    TimeZone.getTimeZone("GMT+8"));
            assertEquals("002608311729162608311729360000000000000000020000", ByteBufUtil.hexDump(data));
        } finally {
            data.release();
        }
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
