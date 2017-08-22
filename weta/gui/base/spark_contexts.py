class SparkEnvironment:
    _sc = None
    _hc = None

    @property
    def sc(self):
        return SparkEnvironment._sc

    @sc.setter
    def sc(self, val):
        SparkEnvironment._sc = val

    @property
    def hc(self):
        return SparkEnvironment._hc

    @hc.setter
    def hc(self, val):
        SparkEnvironment._hc = val
