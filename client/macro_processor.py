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


from argparse import Namespace
from yaml.loader import SafeLoader
from os.path import dirname, isfile, join
from typing import Dict


class MacrosProcessor:
    """A processor to handle macros in the query files during the pre-processing and post-processing stages of a Batch
    Sql Translation job.

    """
    def __init__(self, context: Namespace):
        self.context = context
        if context.macro_map:
            self.enable_macro_substitution = True
            self.macros_subst = MapBasedExpander(context.macro_map)
        else:
            self.enable_macro_substitution = False

    def pre_process(self, input_dir: str, output_dir: str):
        assert self.enable_macro_substitution, "Macros processing not enabled."
        self.process_inputs(input_dir, output_dir)

    def post_process(self, input_dir: str, output_dir: str):
        assert self.enable_macro_substitution, "Macros processing not enabled."
        self.process_inputs(input_dir, output_dir, use_reversed_map=True)

    def process_inputs(self, input_dir: str, output_dir: str, use_reversed_map=False):
        """Replaces macros with values for every file in the input folder and save outputs in a new folder.  Macro
        replacement doesn't apply for files with extension of .zip, .json, and .csv.

        Args:
            input_dir: path to the input directory.
            output_dir: path to the output directory.
            use_reversed_map: whether to use the reversed macro_replacement_maps.
        """
        for file in os.listdir(input_dir):
            file_path = join(input_dir, file)
            if isfile(file_path):
                if not file.lower().endswith(('.zip', '.json', '.csv')) and not file.startswith("."):
                    print("Applying token maps to file %s" % file_path)
                    self.process_file(file_path, input_dir, output_dir, use_reversed_map)
                elif not file.startswith("."):
                    shutil.copy(file_path, file_path.replace(input_dir, output_dir))

    def process_file(self, file_path: str, input_dir: str, output_dir: str, use_reversed_map):
        """Applies the macro maps to a specific file and saves the output file in the output directory with the same
        subdirectory and name as the input file.
        """
        assert isfile(file_path), "Can't find a file at \"%s\"." % file_path

        # Prepare the output file path.
        output_file_path = file_path.replace(input_dir, output_dir)
        # Creates a new output directory if not exists.
        os.makedirs(dirname(output_file_path), exist_ok=True)
        # Creates a write_stream.
        write_stream = open(output_file_path, "w")
        # Prepares the available regex patterns for this file.
        reg_pattern_map, patterns = self.get_all_regex_pattern_mapping(file_path, input_dir, use_reversed_map)
        with open(file_path) as f:
            for line in f:
                line = self.process_text(line, reg_pattern_map, patterns)
                write_stream.write(line)
        write_stream.close()

    def process_text(self, input_text, reg_pattern_map, patterns):
        return patterns.sub(lambda m: reg_pattern_map[re.escape(m.group(0))], input_text)

    def get_all_regex_pattern_mapping(self, file_name: str, base_dir: str, use_reversed_map):
        """ Compiles all the macros matched with the file into a single regex pattern.
        """
        macro_subst_maps = self.macros_subst.get_reversed_maps() if use_reversed_map else \
            self.macros_subst.macro_replacement_maps
        reg_pattern_map = {}
        for file_map_key, token_map in macro_subst_maps.items():
            if fnmatch.fnmatch(file_name, join(base_dir, file_map_key)):
                for key, value in token_map.items():
                    reg_pattern_map[re.escape(key)] = value
        all_patterns = re.compile("|".join(reg_pattern_map.keys()))
        return reg_pattern_map, all_patterns


class MapBasedExpander:
    """An util class to handle map based yaml file.

    """
    __YAML_KEY = "macros_replacement_map"

    def __init__(self, yaml_file_path):
        self.yaml_file_path = yaml_file_path
        self.macro_replacement_maps = self.__parse_macros_config_file()

    def get_reversed_maps(self) -> Dict[str, Dict[str, str]]:
        """ Swaps key and value in the macro maps and return the new map.
        """
        reversed_maps = {}
        for file_key, macro_map in self.macro_replacement_maps.items():
            reversed_maps[file_key] = dict((v, k) for k, v in macro_map.items())
        return reversed_maps

    def __parse_macros_config_file(self) -> Dict[str, Dict[str, str]]:
        """Parses the macros mapping yaml file.

        Return:
            macros_replacement_maps: mapping from macros to the replacement string for each file.  {file_name: {macro: replacement}}.
                File name supports wildcard, e.g., with "*.sql", the method will apply the macro map to all the files with
                extension of ".sql".
        """
        assert isfile(self.yaml_file_path), "Can't find a file at \"%s\"." % self.yaml_file_path

        with open(self.yaml_file_path) as f:
            data = yaml.load(f, Loader=SafeLoader)
        self.__validate_macro_file(data)
        return data[self.__YAML_KEY]

    def __validate_macro_file(self, yaml_data):
        """Validates the macro replacement map yaml data.
        """
        assert self.__YAML_KEY in yaml_data, "Missing %s field in %s." % (self.__YAML_KEY, self.yaml_file_path)
        assert yaml_data[self.__YAML_KEY], "The %s is empty in %s." % (self.__YAML_KEY, self.yaml_file_path)
