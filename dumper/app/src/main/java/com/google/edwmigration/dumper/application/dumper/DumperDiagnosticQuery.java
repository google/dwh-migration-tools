/*
 * Copyright 2022-2023 Google LLC
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
package com.google.edwmigration.dumper.application.dumper;

import org.anarres.jdiagnostics.ClassLoaderQuery;
import org.anarres.jdiagnostics.ClassPathQuery;
import org.anarres.jdiagnostics.CompositeQuery;
import org.anarres.jdiagnostics.DOMQuery;
import org.anarres.jdiagnostics.EnvironmentQuery;
import org.anarres.jdiagnostics.JAXPQuery;
import org.anarres.jdiagnostics.ProductMetadataQuery;
import org.anarres.jdiagnostics.SAXQuery;
import org.anarres.jdiagnostics.SystemPropertiesQuery;
import org.anarres.jdiagnostics.ThrowableQuery;
import org.anarres.jdiagnostics.TmpDirQuery;
import org.anarres.jdiagnostics.XalanQuery;
import org.anarres.jdiagnostics.XercesQuery;

/** @author shevek */
public class DumperDiagnosticQuery extends CompositeQuery {

  public DumperDiagnosticQuery(Throwable t) {
    add(new SystemPropertiesQuery());
    add(new SAXQuery());
    add(new DOMQuery());
    add(new JAXPQuery());
    add(new ClassPathQuery());
    // add(new XSLTQuery());
    add(new EnvironmentQuery());
    // add(new AntQuery());
    add(new XalanQuery());
    add(new XercesQuery());
    add(new TmpDirQuery());
    // add(new ProcessEnvironmentQuery());
    add(new ProductMetadataQuery());
    add(new ThrowableQuery(t));

    add(new ClassLoaderQuery("system", String.class.getClassLoader()));
    add(new ClassLoaderQuery("threadcontext", Thread.currentThread().getContextClassLoader()));
    add(new ClassLoaderQuery("jdiagnostics", getClass().getClassLoader()));
  }
}
