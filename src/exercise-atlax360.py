import sys

from typing import List

from libs.DBExtractor import DBExtractor



def main(args: List[str]):
    try:        
        extractor = DBExtractor("./config/exercise-atlax360.json")
    except Exception:
        print("error locating configuration file ./config/exercise-atlax360.json")

    if len(args) != 1: print("missing required argument target file")
    else: extractor.extract(args[0])



main(sys.argv[1:])
