from apache_beam.coders import Coder

class IgnoreUnicode(Coder):
    def encode(self, value):
        return value.encode("utf-8", "ignore")

    def decode(self, value):
        return value.decode("utf-8", "ignore")

    def is_deterministic(self):
        return True