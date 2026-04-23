import csv
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

        i = 0

        for row in csv_reader:

            vector = row[1].split(" ")
            vector = [float(v) for v in vector if v != ""]

            vectors.append(vector)
            ids.append(int(row[0]))

            if len(vectors) == n_vecs_per_block[i]:

                blocks.append((ids, vectors))

                ids = []
                vectors = []

                i += 1

        return blocks
    
IMPLEMENTATION_PARTITIONER = BlockPartitioner