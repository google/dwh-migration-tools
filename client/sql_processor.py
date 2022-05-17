# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import fnmatch
import os
import re
import shutil
import yaml

from yaml.loader import SafeLoader
from os.path import dirname, isfile, join
from typing import Dict


def process_inputs(input_path: str, output_path: str, macro_maps: Dict[str, Dict[str, str]]):
    """Replaces macros with values for every file in the input folder and save outputs in a new folder.  Macro
    replacement doesn't apply for files with extension of .zip, .json, and .csv.

    Args:
        input_path: path to the input directory.
        output_path: path to the output directory.
        macro_maps: mapping from macros to the replacement string for each file.  {file_name: {macro: replacement}}.
            File name supports wildcard, e.g., with "*.sql", the method will apply the macro map to all the files with
            extension of ".sql".
    """
    for file in os.listdir(input_path):
        file_path = join(input_path, file)
        if isfile(file_path):
            if not file.lower().endswith(('.zip', '.json', '.csv')) and not file.startswith("."):
                print("Applying token maps to file %s" % file_path)
                process_macro_maps(file_path, macro_maps, input_path, output_path)
            elif not file.startswith("."):
                shutil.copy(file_path, file_path.replace(input_path, output_path))


def process_macro_maps(file_path: str, macro_maps: Dict[str, Dict[str, str]], input_path: str, output_path: str):
    """Applies the macro maps to a specific file and saves the output under a new folder with the same name.
    """
    assert isfile(file_path), "Can't find a file at \"%s\"." % file_path
    output_file_path = file_path.replace(input_path, output_path)
    os.makedirs(dirname(output_file_path), exist_ok=True)
    write_stream = open(output_file_path, "w")
    reg_pattern_map, patterns = get_all_regex_pattern_mapping(file_path, macro_maps, input_path)
    with open(file_path) as f:
        for line in f:
            line = patterns.sub(lambda m: reg_pattern_map[re.escape(m.group(0))], line)
            write_stream.write(line)
    write_stream.close()


def get_all_regex_pattern_mapping(file_name: str, macro_maps: Dict[str, Dict[str, str]], base_dir: str):
    """ Compiles all the macros matched with the file into a single regex pattern.
    """
    reg_pattern_map = {}
    for file_map_key, token_map in macro_maps.items():
        if fnmatch.fnmatch(file_name, join(base_dir, file_map_key)):
            for key, value in token_map.items():
                reg_pattern_map[re.escape(key)] = value
    all_patterns = re.compile("|".join(reg_pattern_map.keys()))
    return reg_pattern_map, all_patterns


def get_reverse_maps(macro_maps: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    """ Swaps key and value in the macro maps and return the new map.
    """
    reverse_maps = {}
    for file_key, macro_map in macro_maps.items():
        reverse_maps[file_key] = dict((v, k) for k, v in macro_map.items())
    return reverse_maps


class MacrosMapParser:

    def __init__(self, file_path):
        self.file_path = file_path

    __YAML_KEY = "macros_replacement_map"

    def parse(self) -> Dict[str, Dict[str, str]]:
        """Parses the config file into TranslationConfig.

        Return:
            macros replacement map.
        """
        assert isfile(self.file_path), "Can't find a file at \"%s\"." % self.file_path

        with open(self.file_path) as f:
            data = yaml.load(f, Loader=SafeLoader)
        self.__validate_macro_file(data)
        return data[self.__YAML_KEY]

    def __validate_macro_file(self, yaml_data):
        """Validates the macro replacement map yaml data.
        """
        assert self.__YAML_KEY in yaml_data, "Missing %s field in %s." % (self.__YAML_KEY, self.file_path)
        assert yaml_data[self.__YAML_KEY], "The %s is empty in %s." % (self.__YAML_KEY, self.file_path)
