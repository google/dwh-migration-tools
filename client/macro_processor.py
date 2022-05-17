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


class MacroProcessor:
    """A processor to handle macros in the query files during the pre-processing and post-processing stages of a Batch
    Sql Translation job.

    """
    def __init__(self, context: Namespace):
        self.context = context
		self.expander = MapBasedExpander(context.macros)

    def preprocess(self, input_dir: str, tmp_dir: str):
		"""The pre-upload entry point of a MacroProcessor

		This method expands customer-specific macros and
		substitutions in the source-language SQL, to make it
		valid for the compiler.
		"""
        self.__process(input_dir, tmp_dir)

    def postprocess(self, tmp_dir: str, output_dir: str):
		"""The post-download entry point of a MacroProcessor

		This method re-inserts macros into the generated target-language SQL, if required.
		"""
        self.__process(tmp_dir, output_dir, use_reversed_map=True)

	def is_ignored(self, path, name):
		"""Returns true if a file is ignored.

		Ignored files are not transpiled or copied to the output directory.
		"""
		if (!isfile(path))
			return True
		if (name.startswith("."))
			return True
		return False

	def is_processable(self, path, name):
		"""Returns true if a file is preprocessable.

		Preprocessable files are subject to macro expansion and (optionally) unexpansion.
		Non-preprocessable files are transpiled verbatim. To ignore a file entirely, modify is_ignored.
		"""
		if (is_ignored(path))
			return False
		if name.lower().endswith(('.zip', '.json', '.csv')):
			return False
		return True

    def __process(self, input_dir: str, output_dir: str, use_reversed_map=False):
        """Replaces or restores macros for every file in the input folder and save outputs in a new folder.
		Macro replacement doesn't apply for files which are ignored, or not processable.
		Note that this method is called for varying combinations of input and output directories
		at different points in the process.

        Args:
            input_dir: path to the input directory.
            output_dir: path to the output directory.
            use_reversed_map: whether to use the reversed macro_replacement_maps.
        """
		# TODO: This needs to be a recursive walk using os.walk
        for name in os.listdir(input_dir):
            input_path = join(input_dir, name)
			output_path = join(output_dir, name)
			if is_ignored(input_path):
				continue
			os.makedirs(dirname(output_path), exist_ok=True)
			if not is_processable(input_path):
				shutil.copy(input_path, output_path)
				continue
			# The user may implement entirely different logic for macro expansion
			# vs unexpansion, especially if they are migrating between systems,
			# so we use our boolean flag to separate the paths again here.
			if not use_reversed_map:
				self.preprocess_file(file_path, input_dir, output_dir, use_reversed_map)
			else
				self.postprocess_file(file_path, input_dir, output_dir, use_reversed_map)

	def preprocess_file(self, input_path: str, tmp_path: str):
		print("Preprocessing %s" % input_path)
		with open(input_path) as input_fh:
			text = input_fh.read()
		text = preprocess_text(text, input_path)
		with open(tmp_path) as tmp_fh:
			tmp_fh.write(text)

	def preprocess_text(self, text: str, input_path: str):
		return self.expander.expand(text, input_path, False)

	def postprocess_file(self, tmp_path: str, output_path: str):
		"""Postprocesses the given file, after conversion to the target dialect.

		The user may replace this method with any locally-specified implementation.
		If only simple textual replacement is required, it may be easier to modify postprocess_text.

		Not all users will want postprocessing, and some may just copy the file.
		"""
		print("Postprocessing into %s" % output_path)
		with open(tmp_path) as tmp_fh:
			text = input_fh.read()
		text = postprocess_text(text, output_path)
		with open(output_path) as output_fh:
			output_fh.write(text)

	def postprocess_text(self, text: str, output_path: str):
		"""Postprocesses the given text, after conversion to the target dialect.

		The user may replace this method with any locally-specified implementation.
		If access to the file is required, modify postprocess_file instead, and (optionally) delete this method.

		Not all users will want postprocessing, and some may just return text.
		"""
		return self.expander.expand(text, output_path, True)


class MapBasedExpander:
    """An util class to handle map based yaml file.

    """
    __YAML_KEY = "macros_replacement_map"

    def __init__(self, yaml_file_path):
        self.yaml_file_path = yaml_file_path
        self.macro_replacement_maps = self.__parse_macros_config_file()
		self. #TODO: Precompile all the regexes.

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

    def __get_all_regex_pattern_mapping(self, file_name: str, base_dir: str, use_reversed_map):
        """ Compiles all the macros matched with the file into a single regex pattern.
        """
        macro_subst_maps = self.expander.get_reversed_maps() if use_reversed_map else \
            self.expander.macro_replacement_maps
        reg_pattern_map = {}
        for file_map_key, token_map in macro_subst_maps.items():
            if fnmatch.fnmatch(file_name, join(base_dir, file_map_key)):
                for key, value in token_map.items():
                    reg_pattern_map[re.escape(key)] = value
        all_patterns = re.compile("|".join(reg_pattern_map.keys()))
        return reg_pattern_map, all_patterns

	def expand(self, text: str, path: str, use_reversed_map):
		# TODO os.path.filename(path)
		# Using one or other of the cached patterns objects from the constructor:
        # return patterns.sub(lambda m: reg_pattern_map[re.escape(m.group(0))], input_text)
