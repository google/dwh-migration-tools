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

CREATE TABLE my_table(id INTEGER, jsonCol JSON(1000));

INSERT INTO my_table
  (
    1,
    NEW JSON(
      '{"name" : "Cameron", "age" : 24,
        "schools":[
          {"name":"Lake", "type":"elementary"},
          {"name":"Madison", "type":"middle"},
          {"name":"Rancho", "type":"high"},
          {"name":"UCI", "type":"college"}],
      "job":"programmer"}
        '));
          INSERT INTO my_table(
            2,
            NEW JSON(
              '{"name" : "Melissa", "age" : 23,
                "schools":[
                  {"name":"Lake", "type":"elementary"},
                  {"name":"Madison", "type":"middle"},
                  {"name":"Rancho", "type":"high"},
                  {"name":"Mira Costa", "type":"college"}]}
                  '));
                    INSERT INTO my_table(
                      3,
                      NEW JSON(
                        '{"name" : "Alex", "age" : 25,
                          "schools":[
                            {"name":"Lake", "type":"elementary"},
                            {"name":"Madison", "type":"middle"},
                            {"name":"Rancho", "type":"high"},
                            {"name":"CSUSM", "type":"college"}],
                        "job":"CPA"}
                          '));
                            INSERT INTO my_table(
                              4,
                              NEW JSON(
                                '{"name" : "David", "age" : 25,
                                  "schools":[
                                    {"name":"Lake", "type":"elementary"},
                                    {"name":"Madison", "type":"middle"},
                                    {"name":"Rancho", "type":"high"}],
                                "job":"small business owner"} '));
                                SELECT *
                                FROM
                                  JSON_Table(
                                    ON (SELECT id, jsonCol FROM my_table WHERE id = 1)
                                    USING
                                      rowexpr('$.schools[*]')
                                        colexpr(
                                          '[ {"jsonpath" : "$.name",
                                            "type":"CHAR(20)"},
                                          {"jsonpath":"$.type", "type":"VARCHAR(20)"}]
                                            '))
                                            AS JT(
                                              id, schoolName, "type");
