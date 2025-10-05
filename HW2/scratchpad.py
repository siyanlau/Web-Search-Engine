subset_size = 1000
with open("collection.tsv", "r", encoding="utf8") as fin, \
     open("data/marco_subset.tsv", "w", encoding="utf8") as fout:
    for i, line in enumerate(fin):
        if i >= subset_size:
            break
        fout.write(line)