from __future__ import absolute_import
import os
from .version import __VERSION__ as __version__
from .marshaltools import *
from .surveyfields import SurveyFields, ZTFFields
from .filters import load_filters
load_filters()

here = __file__
# basedir = os.path.split(here)[0]
# example_data = os.path.join(basedir, 'example_data')
