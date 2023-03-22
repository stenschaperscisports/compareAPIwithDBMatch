from compare_match import compare_match
import random

def main():
    csv_filename = "../docs/compare_match_"
    # match_ids = random.sample(range(5034295, 5034305), 10)  # generate 10 random match ids
    match_ids = [5445752]  # generate 10 random match ids
    environment = 'test' # prod or test
    for match_id in match_ids:
        compare_match(environment, match_id, csv_filename=csv_filename+str(match_id)+".csv")
    print("find csv files in docs folder.")

if __name__ == "__main__":
    main()


