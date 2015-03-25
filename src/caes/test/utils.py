# -*- coding: utf-8 -*-

import random
import string


def random_string(termlength=10):
    return ''.join(random.choice(string.letters) for _ in range(termlength))