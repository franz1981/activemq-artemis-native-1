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

package org.apache.activemq.artemis.nativo.jlibaio.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.artemis.nativo.jlibaio.LibaioContext;
import org.apache.activemq.artemis.nativo.jlibaio.LibaioFile;
import org.apache.activemq.artemis.nativo.jlibaio.SubmitInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * This test is using a different package from {@link LibaioFile}
 * as I need to validate public methods on the API
 */
public class LibaioStressTest {

   /**
    * This is just an arbitrary number for a number of elements you need to pass to the libaio init method
    * Some of the tests are using half of this number, so if anyone decide to change this please use an even number.
    */
   private static final int LIBAIO_QUEUE_SIZE = 1_000_000;

   @Rule
   public TemporaryFolder temporaryFolder;

   public LibaioContext<MyClass> control;

   @Before
   public void setUpFactory() {
      control = new LibaioContext<>(LIBAIO_QUEUE_SIZE, false, false);
   }

   @After
   public void deleteFactory() {
      control.close();
      validateLibaio();
   }

   public void validateLibaio() {
      Assert.assertEquals(0, LibaioContext.getTotalMaxIO());
   }

   public LibaioStressTest() {
      /*
       *  I didn't use /tmp for three reasons
       *  - Most systems now will use tmpfs which is not compatible with O_DIRECT
       *  - This would fill up /tmp in case of failures.
       *  - target is cleaned up every time you do a mvn clean, so it's safer
       */
      File parent = new File("./target");
      parent.mkdirs();
      temporaryFolder = new TemporaryFolder(parent);
   }

   @Test
   public void testOpen() throws Exception {
      LibaioFile fileDescriptor = control.openFile(temporaryFolder.newFile("test.bin"), true);
      fileDescriptor.close();
   }

   LinkedBlockingDeque<MyClass> deque = new LinkedBlockingDeque(LIBAIO_QUEUE_SIZE + 100);

   class MyClass implements SubmitInfo {

      @Override
      public void onError(int errno, String message) {

      }

      @Override
      public void done() {
         deque.add(this);
      }
   }

   @Test
   public void testSubmitWriteAndRead() throws Exception {

      for (int i = 0; i < LIBAIO_QUEUE_SIZE + 100; i++) {
         deque.add(new MyClass());
      }

      Thread t = new Thread() {
         @Override
         public void run() {
            control.poll();
         }
      };

      t.start();

      Thread t2 = new Thread() {
         @Override
         public void run() {
            while (true) {
               try {
                  Thread.sleep(1000);
               } catch (Exception e) {
               }
               System.gc();
            }
         }
      };

      t2.start();

      LibaioFile fileDescriptor = control.openFile(temporaryFolder.newFile("test.bin"), true);

      // ByteBuffer buffer = ByteBuffer.allocateDirect(4096);
      ByteBuffer buffer = LibaioContext.newAlignedBuffer(4096, 4096);

      int maxSize = 4096 * 1000;
      fileDescriptor.fill(4096, maxSize);
      for (int i = 0; i < 4096; i++) {
         buffer.put((byte) 'a');
      }

      buffer.rewind();

      int pos = 0;

      long count = 0;

      while (true) {
         count ++;

         if (count % 10_000 == 0) {
            System.out.println("Count " + count);
         }
         MyClass myClass = deque.poll();
         fileDescriptor.write(pos, 4096, buffer, myClass);
         pos += 4096;

         if (pos >= maxSize) {
            pos = 0;
         }

      }
   }

}
