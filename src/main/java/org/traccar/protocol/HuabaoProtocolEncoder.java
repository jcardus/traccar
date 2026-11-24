/*
 * Copyright 2017 - 2025 Anton Tananaev (anton@traccar.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.traccar.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.traccar.BaseProtocolEncoder;
import org.traccar.Protocol;
import org.traccar.config.Keys;
import org.traccar.helper.DataConverter;
import org.traccar.helper.DateUtil;
import org.traccar.helper.model.AttributeUtil;
import org.traccar.model.Command;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import java.util.TimeZone;

public class HuabaoProtocolEncoder extends BaseProtocolEncoder {

    public HuabaoProtocolEncoder(Protocol protocol) {
        super(protocol);
    }

    static void encodeVideoRequestData(
            ByteBuf data, String server, int port, int channel, int dataType, int streamType) {
        byte[] serverBytes = server.getBytes(StandardCharsets.US_ASCII);
        data.writeByte(serverBytes.length);
        data.writeBytes(serverBytes);
        data.writeShort(port); // TCP media port
        data.writeShort(port); // UDP media port
        data.writeByte(channel);
        data.writeByte(dataType); // 0 = audio and video, 1 = video only
        data.writeByte(streamType); // 0 = main stream, 1 = sub stream
    }

    // Cameras index SD-card recordings in their own local time, so the video
    // list / playback commands must send timestamps in that zone, matching
    // HuabaoProtocolDecoder's decode of the 0x1205 reply (decoder.timezone,
    // default GMT+8).
    private TimeZone videoTimeZone(long deviceId) {
        String name = AttributeUtil.lookup(getCacheManager(), Keys.DECODER_TIMEZONE, deviceId);
        return TimeZone.getTimeZone(name != null ? name : "GMT+8");
    }

    private static void writeVideoDate(ByteBuf data, String value, TimeZone timeZone) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyMMddHHmmss");
        dateFormat.setTimeZone(timeZone);
        data.writeBytes(DataConverter.parseHex(dateFormat.format(DateUtil.parseDate(value))));
    }

    private static void writeString(ByteBuf data, String value) {
        byte[] bytes = (value != null ? value : "").getBytes(StandardCharsets.US_ASCII);
        data.writeByte(bytes.length);
        data.writeBytes(bytes);
    }

    // JT/T 1078 0x9206 file upload: the device uploads its SD-card recordings for the time range to an FTP
    // server, then reports 0x1206 when done. The network condition is a bitmask of the links the upload may
    // use (bit 0 Wi-Fi, bit 1 LAN, bit 2 3G/4G); it defaults to Wi-Fi only so a bulk upload doesn't use
    // mobile data.
    static int encodeVideoUploadData(ByteBuf data, Command command, TimeZone timeZone) {
        int networkCondition = command.hasAttribute(Command.KEY_NETWORK_CONDITION)
                ? command.getInteger(Command.KEY_NETWORK_CONDITION) : 0x01;
        writeString(data, command.getString(Command.KEY_SERVER));
        data.writeShort(command.getInteger(Command.KEY_PORT));
        writeString(data, command.getString(Command.KEY_USERNAME));
        writeString(data, command.getString(Command.KEY_PASSWORD));
        writeString(data, command.getString(Command.KEY_PATH));
        data.writeByte(command.getInteger(Command.KEY_INDEX)); // logical channel, 0 = all channels
        writeVideoDate(data, command.getString(Command.KEY_START_TIME), timeZone);
        writeVideoDate(data, command.getString(Command.KEY_END_TIME), timeZone);
        data.writeLong(command.hasAttribute(Command.KEY_ALARM_FLAG) ? command.getLong(Command.KEY_ALARM_FLAG) : 0);
        data.writeByte(command.hasAttribute(Command.KEY_RESOURCE_TYPE)
                ? command.getInteger(Command.KEY_RESOURCE_TYPE) : 0);
        data.writeByte(command.hasAttribute(Command.KEY_STREAM_TYPE)
                ? command.getInteger(Command.KEY_STREAM_TYPE) : 0);
        data.writeByte(command.hasAttribute(Command.KEY_STORAGE_TYPE)
                ? command.getInteger(Command.KEY_STORAGE_TYPE) : 0);
        data.writeByte(networkCondition);
        return networkCondition;
    }

    static void encodeVideoListData(
            ByteBuf data, int channel, String startTime, String endTime, long alarmFlag, TimeZone timeZone) {
        data.writeByte(channel); // logical channel, 0 = all channels
        writeVideoDate(data, startTime, timeZone);
        writeVideoDate(data, endTime, timeZone);
        data.writeLong(alarmFlag); // 0 = all alarm types, otherwise a JT/T 1078 alarm bitmask
        data.writeByte(2); // audio and video
        data.writeByte(0); // all stream types
        data.writeByte(0); // all storage types
    }

    @Override
    protected Object encodeCommand(Command command) {

        boolean alternative = AttributeUtil.lookup(
                getCacheManager(), Keys.PROTOCOL_ALTERNATIVE.withPrefix(getProtocolName()), command.getDeviceId());

        ByteBuf id = Unpooled.wrappedBuffer(
                DataConverter.parseHex(getUniqueId(command.getDeviceId())));
        try {
            ByteBuf data = Unpooled.buffer();
            byte[] time = DataConverter.parseHex(new SimpleDateFormat("yyMMddHHmmss").format(new Date()));

            switch (command.getType()) {
                case Command.TYPE_CUSTOM:
                    String model = getDeviceModel(command.getDeviceId());
                    if (model != null && Set.of("AL300", "GL100", "VL300").contains(model)) {
                        data.writeByte(1); // number of parameters
                        data.writeInt(0xF030); // AT command transparent transmission
                        int length = command.getString(Command.KEY_DATA).length();
                        data.writeByte(length);
                        data.writeCharSequence(command.getString(Command.KEY_DATA), StandardCharsets.US_ASCII);
                        return HuabaoProtocolDecoder.formatMessage(
                                0x7e, HuabaoProtocolDecoder.MSG_CONFIGURATION_PARAMETERS, id, false, data);
                    } else if ("BSJ".equals(model)) {
                        data.writeByte(1); // flag
                        var charset = Charset.isSupported("GBK") ? Charset.forName("GBK") : StandardCharsets.US_ASCII;
                        data.writeCharSequence(command.getString(Command.KEY_DATA), charset);
                        return HuabaoProtocolDecoder.formatMessage(
                                0x7e, HuabaoProtocolDecoder.MSG_SEND_TEXT_MESSAGE, id, false, data);
                    } else {
                        return Unpooled.wrappedBuffer(DataConverter.parseHex(command.getString(Command.KEY_DATA)));
                    }
                case Command.TYPE_REBOOT_DEVICE:
                    data.writeByte(1); // number of parameters
                    data.writeByte(0x23); // parameter id
                    data.writeByte(1); // parameter value length
                    data.writeByte(0x03); // restart
                    return HuabaoProtocolDecoder.formatMessage(
                            0x7e, HuabaoProtocolDecoder.MSG_PARAMETER_SETTING, id, false, data);
                case Command.TYPE_POSITION_PERIODIC:
                    data.writeByte(1); // number of parameters
                    data.writeByte(0x06); // parameter id
                    data.writeByte(4); // parameter value length
                    data.writeInt(command.getInteger(Command.KEY_FREQUENCY));
                    return HuabaoProtocolDecoder.formatMessage(
                            0x7e, HuabaoProtocolDecoder.MSG_PARAMETER_SETTING, id, false, data);
                case Command.TYPE_ALARM_ARM:
                case Command.TYPE_ALARM_DISARM:
                    data.writeByte(1); // number of parameters
                    data.writeByte(0x24); // parameter id
                    String username = "user";
                    data.writeByte(1 + username.length()); // parameter value length
                    data.writeByte(command.getType().equals(Command.TYPE_ALARM_ARM) ? 0x01 : 0x00);
                    data.writeCharSequence(username, StandardCharsets.US_ASCII);
                    return HuabaoProtocolDecoder.formatMessage(
                            0x7e, HuabaoProtocolDecoder.MSG_PARAMETER_SETTING, id, false, data);
                case Command.TYPE_ENGINE_STOP:
                case Command.TYPE_ENGINE_RESUME:
                    if (alternative) {
                        data.writeByte(command.getType().equals(Command.TYPE_ENGINE_STOP) ? 0x01 : 0x00);
                        data.writeBytes(time);
                        return HuabaoProtocolDecoder.formatMessage(
                                0x7e, HuabaoProtocolDecoder.MSG_OIL_CONTROL, id, false, data);
                    } else {
                        if ("VL300".equals(getDeviceModel(command.getDeviceId()))) {
                            data.writeCharSequence(command.getType().equals(Command.TYPE_ENGINE_STOP) ? "#0;1" : "#0;0",
                                    StandardCharsets.US_ASCII);
                        } else {
                            data.writeByte(command.getType().equals(Command.TYPE_ENGINE_STOP) ? 0xf0 : 0xf1);
                        }
                        return HuabaoProtocolDecoder.formatMessage(
                                0x7e, HuabaoProtocolDecoder.MSG_TERMINAL_CONTROL, id, false, data);
                    }
                case Command.TYPE_VIDEO_START:
                    // Video only by default: some cameras never open the media connection when audio is requested
                    int dataType = command.hasAttribute(Command.KEY_DATA_TYPE)
                            ? command.getInteger(Command.KEY_DATA_TYPE) : 1;
                    int streamType = command.hasAttribute(Command.KEY_STREAM_TYPE)
                            ? command.getInteger(Command.KEY_STREAM_TYPE) : 0;
                    encodeVideoRequestData(
                            data,
                            command.getString(Command.KEY_SERVER),
                            command.getInteger(Command.KEY_PORT),
                            command.getInteger(Command.KEY_INDEX),
                            dataType,
                            streamType);
                    return HuabaoProtocolDecoder.formatMessage(
                            0x7e, HuabaoProtocolDecoder.MSG_VIDEO_REQUEST, id, false, data);
                case Command.TYPE_VIDEO_STOP:
                    data.writeByte(command.getInteger(Command.KEY_INDEX));
                    data.writeByte(0); // close audio and video
                    data.writeByte(0); // close both audio and video
                    data.writeByte(0); // main stream
                    return HuabaoProtocolDecoder.formatMessage(
                            0x7e, HuabaoProtocolDecoder.MSG_VIDEO_CONTROL, id, false, data);
                case Command.TYPE_VIDEO_LIST:
                    encodeVideoListData(
                            data,
                            command.getInteger(Command.KEY_INDEX),
                            command.getString(Command.KEY_START_TIME),
                            command.getString(Command.KEY_END_TIME),
                            command.getLong(Command.KEY_ALARM_FLAG),
                            videoTimeZone(command.getDeviceId()));
                    return HuabaoProtocolDecoder.formatMessage(
                            0x7e, HuabaoProtocolDecoder.MSG_VIDEO_LIST, id, false, data);
                case Command.TYPE_VIDEO_PLAYBACK:
                    String playbackServer = command.getString(Command.KEY_SERVER);
                    data.writeByte(playbackServer.length());
                    data.writeCharSequence(playbackServer, StandardCharsets.US_ASCII);
                    data.writeShort(command.getInteger(Command.KEY_PORT));
                    data.writeShort(0); // UDP port
                    data.writeByte(command.getInteger(Command.KEY_INDEX));
                    data.writeByte(command.getInteger(Command.KEY_RESOURCE_TYPE));
                    data.writeByte(command.getInteger(Command.KEY_STREAM_TYPE));
                    data.writeByte(command.getInteger(Command.KEY_STORAGE_TYPE));
                    data.writeByte(0); // normal playback
                    data.writeByte(0); // normal speed
                    TimeZone playbackTimeZone = videoTimeZone(command.getDeviceId());
                    writeVideoDate(data, command.getString(Command.KEY_START_TIME), playbackTimeZone);
                    writeVideoDate(data, command.getString(Command.KEY_END_TIME), playbackTimeZone);
                    return HuabaoProtocolDecoder.formatMessage(
                            0x7e, HuabaoProtocolDecoder.MSG_VIDEO_PLAYBACK, id, false, data);
                case Command.TYPE_VIDEO_UPLOAD:
                    encodeVideoUploadData(data, command, videoTimeZone(command.getDeviceId()));
                    return HuabaoProtocolDecoder.formatMessage(
                            0x7e, HuabaoProtocolDecoder.MSG_VIDEO_UPLOAD, id, false, data);
                default:
                    return null;
            }
        } finally {
            id.release();
        }
    }

}
