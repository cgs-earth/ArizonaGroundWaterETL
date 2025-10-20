from xlsx import add_wells_55_metadata, load_xlsx_files  
import argparse

if __name__ == "__main__":

    args = argparse.ArgumentParser()
    args.add_argument("--xlsx", action="store_true")
    args.add_argument("--wells55", action="store_true")
    args = args.parse_args()


    if args.xlsx:
        load_xlsx_files()
    elif args.wells55:
        add_wells_55_metadata()
    else:
        raise Exception("No arguments provided. You must load either xlsx or wells55 metadata")