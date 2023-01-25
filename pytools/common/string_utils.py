import hashlib
import random
import re
import string
import warnings
from typing import Any, Iterator, List, Optional, Set, Tuple

with warnings.catch_warnings():
    # We deliberately do not install the C-based `python-Levenshtein` module, so suppress warning:
    warnings.filterwarnings("ignore", message=r"^Using slow pure-python SequenceMatcher")
    from fuzzywuzzy import fuzz

from pytools.common.datetime_utils import format_iso_datetime_for_mysql
from pytools.common.logger import Logger

SNAKE_CASE_PATTERN = re.compile(r"[_-]\S")
SNAKE_CASE_REPLACEMENT_SYMBOLS = re.compile("[_-]")


class StringUtils:
    STRING_FALSE_VALUES = {"0", "false", "no", "none", "null"}

    @staticmethod
    def get_hash_prefix(mystr: str) -> str:
        """
        Get first 4 characters of a hexadecimal `mystr` hash as a string.

        Arguments:
            mystr - Value to hash.

        Returns:
            A string like "a94f".
        """
        hash_object = hashlib.sha1(mystr.encode("utf-8"))
        hex_dig = hash_object.hexdigest()

        return hex_dig[:4]

    @staticmethod
    def str_to_bool(mystr: Optional[str]) -> bool:
        """
        Convert a string or None to bolean value.

        Arguments:
            mystr -- String to convert.

        Returns:
            True if `mystr` is True-ish, False otherwise
        """
        if mystr is None:
            return False

        if mystr.lower() in StringUtils.STRING_FALSE_VALUES:
            return False

        return True

    @staticmethod
    def param_normalizer(mystr: Any) -> Optional[str]:
        """
        Normalizes input parameters that come from front-end applications.

        Arguments:
            mystr -- Input parameter from front-end.

        Returns:
            Normalized string that is usable by the backend python application.
        """
        if isinstance(mystr, int):
            mystr = str(mystr)

        if (mystr is None) or (mystr.lower() in ("none", "null", "")):
            return None
        if mystr in ("datetime", "dateTime"):
            return "date_time"

        return mystr

    @staticmethod
    def list_of_str(mystr: Any) -> List[Any]:
        """
        Return a list with one item `mystr`. If `mysql` is a list - return it as it is.

        Arguments:
            mystr -- Value to check.

        Returns:
            A list with one item or original list.
        """
        if isinstance(mystr, list):
            return mystr

        return [mystr]

    @staticmethod
    def format_for_mysql(value: Any) -> str:
        """
        Convert any `value` to MySQL format.

        Arguments:
            value -- Value to convert.

        Returns:
            A string in format "'my_value'" or "NULL".
        """
        if value is None or value == "None":
            return "NULL"

        if isinstance(value, bool):
            value = str(value)

        # remove the first and last quote characters.
        if value.startswith("'") and value.endswith("'"):
            value = value[1:-1]
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]

        value = value.replace("'", "\\'").replace('"', '\\"')
        return f"'{value}'"

    @staticmethod
    def format_datetime_for_mysql(dt_string: Optional[str]) -> str:
        """
        Convert a string with datetime to MySQL format.

        Arguments:
            dt_string -- A string representing a datetime in any parseable format

        Returns:
            A string representing a datetime value in MySQL format, e.g.
            "'2019-03-08 18:49:23'", or "NULL".
        """
        if dt_string is None or dt_string == "None":
            return "NULL"

        return f"'{format_iso_datetime_for_mysql(dt_string)}'"

    @staticmethod
    def convert_bool_to_int(value: Any) -> str:
        """
        Convert a value to a string '0' for false-ish `value`, or '1' for true-ish `value`

        Arguments:
            value -- Value to check

        Returns:
            A string '0' or '1'.
        """
        return str(int(value in ["True", True]))

    @staticmethod
    def ascii_string_generator(length: int = 3) -> Iterator[str]:
        """
        Generator to build unique strings from "aa...a" to "zz...z".

        ```python
        gen = StringUtils.ascii_string_generator()
        next(gen)  # 'aaa'
        next(gen)  # 'aab'
        list(gen)[-1]  # 'zzz'
        ```

        Arguments:
            length - - Length of a result string.

        Yields:
            Lowercased ASCII string like "aaa"
        """
        counter = 0
        letters = string.ascii_lowercase
        letters_length = len(letters)
        max_counter = letters_length**length
        while counter < max_counter:
            result = []
            for pos in range(length - 1, -1, -1):
                letter_index = counter // (letters_length**pos) % letters_length
                result.append(letters[letter_index])
            counter += 1
            yield "".join(result)

    @staticmethod
    def pluralize(count: int, singular: str, plural: Optional[str] = None) -> str:
        """
        Pluralize a noun according to `count`.

        Arguments:
            count -- Count of objects.
            singular -- Singular noun form.
            plural -- Plural noun form. If not provided - append `s` to singular form.

        Returns:
            A noun in proper form.
        """
        if count == 1:
            return singular

        if plural is not None:
            return plural

        return f"{singular}s"

    @staticmethod
    def get_format_keys(format_string: str) -> Set[str]:
        """
        Extract format keys from a formet-ready string.

        ```python
        keys = StringUtils.get_format_keys('key: {key} {value}')
        keys # ['key', 'value']
        ```

        Arguments:
            format_string -- A format-ready string.

        Returns:
            A set of format keys.
        """
        result = set()
        formatter = string.Formatter()
        for _, key, _, _ in formatter.parse(format_string):
            if key:
                result.add(key)

        return result

    @staticmethod
    def generate_random_string(
        length: int, *character_lists: str, choices: str = string.ascii_letters
    ) -> str:
        """
        Generate a string of given `length` with characters from `choices`.
        Can be used for password generation.

        At least one character from `character_lists` will appear in result
        if `length` is enough.

        ```python
        StringUtils.generate_random_string(5) # 'XHdcS'
        StringUtils.generate_random_string(8, '123') # '32221131'
        StringUtils.generate_random_string(8, '123', '&!') # '22!11&31'
        ```

        Arguments:
            length -- Result string length.
            character_lists -- Lists of chars used for string generation.
            choices -- Deprecated.

        Returns:
            A random string.
        """
        result: List[str] = []
        symbol: str
        if not character_lists:
            character_lists = (choices,)

        full_character_list: List[str] = []
        for character_list in character_lists:
            full_character_list.extend(character_list)

        character_list_index = 0
        while len(result) < length:
            if character_list_index < len(character_lists):
                current_choices = character_lists[character_list_index]
                symbol = random.choice(current_choices)
                result.append(symbol)
                character_list_index = character_list_index + 1
                continue

            symbol = random.choice(full_character_list)
            result.append(symbol)

        random.shuffle(result)
        return "".join(result)

    @classmethod
    def generate_db_password(cls) -> str:
        """
        Generate a 18-char password meeting MySQL password policy.

        Retuns:
            A password from MySQL.
        """
        return cls.generate_random_string(
            18,
            string.ascii_lowercase,
            string.ascii_uppercase,
            string.digits,
            "!#$%&()*+,-.:<=>?[]^_{|}~",
        )

    @staticmethod
    def remove_control_chars(original_str: str) -> str:
        """
        Removes control characters such as tab, nextline from a string
        Args:
            original_str: the original string object containing control chars

        Returns:
            a string with its control chars removed
        """
        control_chars = "".join(map(chr, list(range(0, 32)) + list(range(127, 160))))
        control_char_re = re.compile("[%s]" % re.escape(control_chars))
        return control_char_re.sub("", original_str)

    @staticmethod
    def convert_camel_case_to_snake_case(original_str: str) -> str:
        return "".join(["_" + i.lower() if i.isupper() else i for i in original_str]).lstrip("_")

    @staticmethod
    def convert_snake_case_to_camel_case(original_str: str) -> str:
        first_char = original_str[0] if len(original_str) > 0 else original_str
        partial_str = original_str[1:] if len(original_str) > 1 else ""
        return first_char.lower() + SNAKE_CASE_PATTERN.sub(
            repl=lambda pat: SNAKE_CASE_REPLACEMENT_SYMBOLS.sub("", pat[0]).upper(),
            string=partial_str,
        )

    @staticmethod
    def get_strings_similarity_score(string_a: str, string_b: str) -> int:
        """Gets similarity ratio score between two strings.
        Args:
            string_a: String to be compared.
            string_b:String to be compared.
        Returns:
            similarity score
        """
        partial_ratio = 0
        similarities_values = []
        string_a_len = len(string_a)
        string_b_len = len(string_b)

        if (string_a_len <= 4 or string_b_len <= 4) and string_a != string_b:
            return 0

        if string_a_len >= 5 and string_b_len >= 5:
            partial_ratio = fuzz.partial_ratio(string_a, string_b)
            similarities_values.append(partial_ratio)

        ratio = fuzz.ratio(string_a, string_b)
        similarities_values.append(ratio)
        sort_ratio = fuzz.token_sort_ratio(string_a, string_b)
        similarities_values.append(sort_ratio)

        Logger(__name__).debug(
            f"Ratio result {string_a} and {string_b} "
            f"Partial Ratio: {partial_ratio}, "
            f"Sort Ratio: {sort_ratio}"
        )
        return max(*similarities_values)

    @staticmethod
    def find_match_in_list(
        string_a: str, string_list: List[str], min_ratio: int
    ) -> List[Tuple[str, str, int]]:
        """Find matches between two strings with similarity scores.
        Args:
            string_a: String to compare.
            string_list: List of strings to compare against.
            min_ratio: Threshold to be considered as a match.
        Returns:
            List with matching result
        """
        match_result = []
        for string_to_compare in string_list:
            ratio = StringUtils.get_strings_similarity_score(string_a, string_to_compare)

            if ratio >= min_ratio:
                match_result.append((string_a, string_to_compare, ratio))

        return match_result


def main() -> None:
    logger = Logger(__name__, level=Logger.DEBUG)
    str_tools = StringUtils()
    logger.debug(f'{str_tools.list_of_str("test")}')
    logger.debug(f'{str_tools.list_of_str(["test"])}')
    logger.debug(f'{str_tools.list_of_str(["test1", "test2"])}')
    logger.debug(f'{str_tools.list_of_str(list("test"))}')
    clean_str = StringUtils.remove_control_chars("\ttest\n")
    logger.debug(f"{clean_str}")
    logger.debug(StringUtils.convert_camel_case_to_snake_case("TestRun"))


if __name__ == "__main__":
    main()
