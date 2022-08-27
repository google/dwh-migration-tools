-- Copyright 2022 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

SELECT to_char(date '2003-12-23', 'dd-Mon-YYYY');

SELECT to_char(date '2003-12-23');

SELECT to_char(time '10:44:25', 'hh:mm:ss');

SELECT to_char(time '10:44:25');

SELECT to_char(timestamp '2003-12-23 10:44:01', 'dd-Mon-YYYY hh:mm:ss');

SELECT to_char(timestamp '2003-12-23 10:44:02');

SELECT to_char(INTERVAL '1' hour);

SELECT to_char(CAST(timestamp '2003-12-23 10:44:25' AS timestamp WITH time zone));

SELECT to_char(CAST(time '10:44:25' AS time WITH time zone));
