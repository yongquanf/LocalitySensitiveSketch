package util.bloom.Apache.Hash;

import util.bloom.Apache.Hash.CityAndFarmHash_1_1.AsLongHashFunction;

public   class AsLongHashFunctionSeeded extends AsLongHashFunction {
    private final long seed;
    private final long voidHash;
    // Primes if treated as unsigned
    private static final long P1 = -7046029288634856825L;
    private static final long P2 = -4417276706812531889L;
    private static final long P3 = 1609587929392839161L;
    private static final long P4 = -8796714831421723037L;
    private static final long P5 = 2870177450012600261L;

    private AsLongHashFunctionSeeded(long seed) {
        this.seed = seed;
        voidHash = XxHash.finalize(seed + P5);
    }

    public long seed() {
        return seed;
    }

    @Override
    public long hashVoid() {
        return voidHash;
    }
}
