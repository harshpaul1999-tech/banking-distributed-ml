"""Read bank.csv data records safely from arbitrary Hadoop input splits."""
import csv

FIELDS = ["age", "job", "marital", "education", "default", "balance",
          "housing", "loan", "contact", "day", "month", "duration",
          "campaign", "pdays", "previous", "poutcome", "y"]


def read_bank_rows(stream):
    for values in csv.reader(stream):
        if not values or values[0] == "age":
            continue
        if len(values) != len(FIELDS):
            continue
        yield dict(zip(FIELDS, values))
