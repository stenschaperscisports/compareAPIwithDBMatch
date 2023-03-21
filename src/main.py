from compare_match import compare_match

def main():
    csv_filename = "../docs/compare_match.csv"
    match_ids = [5034295,5034295]
    for match_id in match_ids:
        compare_match(match_id, csv_filename)

if __name__ == "__main__":
    main()