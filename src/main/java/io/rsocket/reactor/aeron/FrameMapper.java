package io.rsocket.reactor.aeron;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import reactor.aeron.DirectBufferHandler;

import java.nio.ByteBuffer;
import java.util.function.Function;

public final class FrameMapper
        implements DirectBufferHandler<ByteBuf>, Function<DirectBuffer, ByteBuf> {

    @Override
    public int estimateLength(ByteBuf frame) {
        return frame.readableBytes();
    }

    public void write(MutableDirectBuffer dstBuffer, int offset, ByteBuf frame, int length) {
        dstBuffer.putBytes(offset, frame.nioBuffer(), length);
    }

    @Override
    public DirectBuffer map(ByteBuf frame, int length) {
        return new UnsafeBuffer(frame.nioBuffer(), 0, length);
    }

    @Override
    public void dispose(ByteBuf frame) {
        frame.release();
    }

    @Override
    public ByteBuf apply(DirectBuffer srcBuffer) {
        int capacity = srcBuffer.capacity();
        ByteBuffer dstBuffer = ByteBuffer.allocate(capacity);
        srcBuffer.getBytes(0, dstBuffer, capacity);
        dstBuffer.rewind();
        return Unpooled.wrappedBuffer(dstBuffer);
    }
}
