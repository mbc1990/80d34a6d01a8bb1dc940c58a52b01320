
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('domain')
    args = parser.parse_args()
    domain = args.domain

if __name__ == "__main__":
    main()
