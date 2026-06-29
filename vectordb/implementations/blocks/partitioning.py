import csv
import json
from io import StringIO


class BlockPartitioner:

    def __init__(self, n_blocks):
        self.n_blocks = n_blocks

    def partition(self, csv_data):

        csv_buffer = StringIO(csv_data)
        csv_reader = list(csv.reader(csv_buffer))

        quotient, remainder = divmod(len(csv_reader), self.n_blocks)

        lower = [quotient for _ in range(self.n_blocks - remainder)]
        higher = [quotient + 1 for _ in range(remainder)]

        n_vecs_per_block = lower + higher

        blocks = []
        vectors = []
        ids = []
        tags_list = []

        i = 0

        for row in csv_reader:

            vector = row[1].split(" ")
            vector = [float(v) for v in vector if v != ""]

            vectors.append(vector)
            vid = int(row[0])
            ids.append(vid)

            tags = None
            if len(row) > 2 and row[2].strip():
                try:
                    t = json.loads(row[2])
                    if isinstance(t, dict):
                        tags = t
                except (json.JSONDecodeError, ValueError):
                    pass
            tags_list.append(tags)

            if len(vectors) == n_vecs_per_block[i]:
                tags_dict = {}
                for j, t in enumerate(tags_list):
                    if t:
                        tags_dict[str(ids[j])] = t
                blocks.append((ids, vectors, tags_dict))

                ids = []
                vectors = []
                tags_list = []

                i += 1

        return blocks
    
IMPLEMENTATION_PARTITIONER = BlockPartitioner