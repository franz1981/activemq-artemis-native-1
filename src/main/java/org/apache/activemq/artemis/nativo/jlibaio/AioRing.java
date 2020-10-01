/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.nativo.jlibaio;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

import sun.misc.Unsafe;

/**
 * Unsafe access representation for:
 * <pre>
 * struct aio_ring {
 *    unsigned	id;
 *    unsigned nr;
 *    unsigned head;
 *    unsigned tail;
 *    unsigned magic;
 *    unsigned compat_features;
 *    unsigned incompat_features;
 *    unsigned header_length;
 *
 *    struct io_event io_events[0];
 * };
 * </pre>
 * <p>
 * Support is just for 64 bits for now ie sizeof(unsigned) == Integer.BYTES
 */
public final class AioRing {

   private static final int AIO_RING_MAGIC = 0xa10a10a1;
   private static final int AIO_RING_INCOMPAT_FEATURES = 0;
   private static final int ID_OFFSET = 0;
   private static final int NR_OFFSET = ID_OFFSET + Integer.BYTES;
   private static final int HEAD_OFFSET = NR_OFFSET + Integer.BYTES;
   private static final int TAIL_OFFSET = HEAD_OFFSET + Integer.BYTES;
   private static final int MAGIC_OFFSET = TAIL_OFFSET + Integer.BYTES;
   private static final int COMPAT_FEATURES_OFFSET = MAGIC_OFFSET + Integer.BYTES;
   private static final int INCOMPAT_FEATURES_OFFSET = COMPAT_FEATURES_OFFSET + Integer.BYTES;
   private static final int HEADER_LENGTH_OFFSET = INCOMPAT_FEATURES_OFFSET + Integer.BYTES;
   private static final int IO_EVENT_OFFSET = HEADER_LENGTH_OFFSET + Integer.BYTES;
   public static final int SIZE_OF_AIO_RING = IO_EVENT_OFFSET + Long.BYTES;
   /**
    * <pre>
    * struct io_event {
    *    __u64		data;
    *    __u64 obj;
    *    __s64 res;
    *    __s64 res2;
    * };
    * </pre>
    */
   private static final int SIZE_OF_IO_EVENT_STRUCT = 32;

   private static final Unsafe UNSAFE;

   static {
      UNSAFE = getUnsafe();
   }

   private static Unsafe getUnsafe() {
      Unsafe instance;
      try {
         final Field field = Unsafe.class.getDeclaredField("theUnsafe");
         field.setAccessible(true);
         instance = (Unsafe) field.get(null);
      } catch (Exception ignored) {
         // Some platforms, notably Android, might not have a sun.misc.Unsafe implementation with a private
         // `theUnsafe` static instance. In this case we can try to call the default constructor, which is sufficient
         // for Android usage.
         try {
            Constructor<Unsafe> c = Unsafe.class.getDeclaredConstructor();
            c.setAccessible(true);
            instance = c.newInstance();
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }
      return instance;
   }

   private final long aioRingAddress;
   private final int nr;

   public AioRing(long aioRingAddress) {
      if (!hasUsableRing(aioRingAddress)) {
         throw new IllegalStateException("Unsafe kernel bypass cannot be used!");
      }
      this.aioRingAddress = aioRingAddress;
      this.nr = UNSAFE.getInt(aioRingAddress + NR_OFFSET);
      if (nr <= 0) {
         throw new IllegalStateException("the ring buffer has nr = " + nr);
      }
   }

   public static boolean hasUsableRing(long aioRingAddress) {
      return UNSAFE.getInt(aioRingAddress + MAGIC_OFFSET) == AIO_RING_MAGIC &&
         UNSAFE.getInt(aioRingAddress + INCOMPAT_FEATURES_OFFSET) == AIO_RING_INCOMPAT_FEATURES;
   }

   public int size() {
      final long aioRingAddress = this.aioRingAddress;
      final int nr = this.nr;
      final int head = UNSAFE.getInt(aioRingAddress + HEAD_OFFSET);
      // no need of membar here because UNSAFE::getInt already provide it
      final int tail = UNSAFE.getIntVolatile(null, aioRingAddress + TAIL_OFFSET);
      int available = tail - head;
      if (available < 0) {
         available += nr;
      }
      // this is to mitigate a RHEL BUG: see native code for more info
      if (available > nr) {
         return 0;
      }
      return available;
   }

   public int poll(long completedIoEvents) {
      final long aioRingAddress = this.aioRingAddress;
      final int nr = this.nr;
      int head = UNSAFE.getInt(aioRingAddress + HEAD_OFFSET);
      // no need of membar here because UNSAFE::getInt already provide it
      final int tail = UNSAFE.getIntVolatile(null, aioRingAddress + TAIL_OFFSET);
      int available = tail - head;
      if (available < 0) {
         available += nr;
      }
      // this is to mitigate a RHEL BUG: see native code for more info
      if (available > nr) {
         return 0;
      }
      final long ringIoEvents = aioRingAddress + IO_EVENT_OFFSET;
      for (int i = 0; i < available; i++) {
         // not very pipeline friendly, but seems that we should split this loop into 2 memcpy
         // if we want to save from using % -> aio seems that can get nr not power of 2!
         head = head >= nr ? 0 : head;
         // this could be smarter
         final long ringAioEvent = ringIoEvents + (head * SIZE_OF_IO_EVENT_STRUCT);
         // probably using 3 Unsafe::putLong is better the use memcpy-like instr
         UNSAFE.copyMemory(ringAioEvent, completedIoEvents, SIZE_OF_IO_EVENT_STRUCT);
         completedIoEvents += SIZE_OF_IO_EVENT_STRUCT;
         head++;
      }
      // it allow the kernel to build its own view of the ring buffer size
      // and push new events if there are any
      UNSAFE.putOrderedInt(null, aioRingAddress + HEAD_OFFSET, head);
      return available;
   }

   public int peek(long completedIoEvents) {
      final long aioRingAddress = this.aioRingAddress;
      final int nr = this.nr;
      int head = UNSAFE.getInt(aioRingAddress + HEAD_OFFSET);
      // no need of membar here because UNSAFE::getInt already provide it
      final int tail = UNSAFE.getIntVolatile(null, aioRingAddress + TAIL_OFFSET);
      int available = tail - head;
      if (available < 0) {
         available += nr;
      }
      // this is to mitigate a RHEL BUG: see native code for more info
      if (available > nr) {
         return 0;
      }
      final long ringIoEvents = aioRingAddress + IO_EVENT_OFFSET;
      for (int i = 0; i < available; i++) {
         head = head >= nr ? 0 : head;
         // this could be smarter
         final long ringAioEvent = ringIoEvents + (head * SIZE_OF_IO_EVENT_STRUCT);
         // probably using 3 Unsafe::putLong is better the use memcpy-like instr
         UNSAFE.copyMemory(ringAioEvent, completedIoEvents, SIZE_OF_IO_EVENT_STRUCT);
         completedIoEvents += SIZE_OF_IO_EVENT_STRUCT;
      }
      return available;
   }

}
