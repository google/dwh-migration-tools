/*
 * Copyright 2022-2025 Google LLC
 * Copyright 2013-2021 CompilerWorks
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
package com.google.edwmigration.dumper.application.dumper.connector.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HdfsHandleTest {

  @Test
  public void isLoginFailure_loginExceptionCause_returnsTrue() {
    assertTrue(
        HdfsHandle.isLoginFailure(
            new IOException(
                "Invalid UID, could not determine effective user",
                new LoginException("java.lang.NullPointerException: invalid null input: name"))));
    assertTrue(
        HdfsHandle.isLoginFailure(
            new IOException("failure to login", new FailedLoginException("os identity"))));
  }

  @Test
  public void isLoginFailure_otherIoException_returnsFalse() {
    assertFalse(HdfsHandle.isLoginFailure(new IOException("Exception reading token file")));
    assertFalse(
        HdfsHandle.isLoginFailure(
            new IOException("Exception reading token file", new EOFException())));
  }

  @Test
  public void fallbackUserName_hadoopUserNameEnv_isPreferred() {
    assertEquals("alice", HdfsHandle.fallbackUserName("alice", "dave", "bob", "carol"));
  }

  @Test
  public void fallbackUserName_noHadoopUserNameEnv_returnsHadoopUserNameProperty() {
    assertEquals("dave", HdfsHandle.fallbackUserName(null, "dave", "bob", "carol"));
    assertEquals("dave", HdfsHandle.fallbackUserName("", "dave", "bob", "carol"));
  }

  @Test
  public void fallbackUserName_noHadoopUserName_returnsUser() {
    assertEquals("bob", HdfsHandle.fallbackUserName(null, null, "bob", "carol"));
    assertEquals("bob", HdfsHandle.fallbackUserName("", "", "bob", "carol"));
  }

  @Test
  public void fallbackUserName_onlyUserNameProperty_returnsIt() {
    assertEquals("carol", HdfsHandle.fallbackUserName(null, null, null, "carol"));
    assertEquals("carol", HdfsHandle.fallbackUserName("", "", "", "carol"));
  }

  @Test
  public void fallbackUserName_noName_returnsNull() {
    assertNull(HdfsHandle.fallbackUserName(null, null, null, null));
    assertNull(HdfsHandle.fallbackUserName("", "", "", ""));
    // Java reports "?" for a UID without a user name.
    assertNull(HdfsHandle.fallbackUserName(null, null, null, "?"));
  }
}
