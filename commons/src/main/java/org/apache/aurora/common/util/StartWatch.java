/**
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
//************************************************************************
//
//                      Summize
//
// This work protected by US Copyright Law and contains proprietary and
// confidential trade secrets.
//
// (c) Copyright 2007 Summize,  ALL RIGHTS RESERVED.
//
//************************************************************************

package org.apache.aurora.common.util;

import org.apache.commons.lang.time.StopWatch;

public class StartWatch extends StopWatch {
  public StartWatch() {
    super();
  }

  public void start() {
    _started = true;
    super.start();
  }

  public void resume() {
    if (!_started) {
      start();
    } else {
      super.resume();
    }
  }

  private boolean _started = false;
}
