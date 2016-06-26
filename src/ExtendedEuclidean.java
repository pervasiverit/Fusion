import java.math.BigInteger;

public class ExtendedEuclidean {
	
	private static BigInteger[] calculateEuclidean(BigInteger p, BigInteger q) {
        if (q.equals(BigInteger.ZERO)) {
            return new BigInteger[]{p, BigInteger.ONE, BigInteger.ZERO};
        } else {
            BigInteger[] vals = calculateEuclidean(q, p.remainder(q));
            BigInteger d = vals[0];
            BigInteger a = vals[2];
            BigInteger b = vals[1].subtract(p.divide(q).multiply(vals[2]));
            return new BigInteger[]{d, a, b};
        }
    }
	
	public static BigInteger moduloInverse(BigInteger k, BigInteger n) {
        BigInteger[] vals = calculateEuclidean(k, n);
        BigInteger d = vals[0];
        BigInteger a = vals[1];

        if (d.compareTo(BigInteger.ONE) == 1) {
            return BigInteger.ZERO;
        }

        if (a.compareTo(BigInteger.ZERO) == 1) {
            return a;
        }
        
        return n.add(a);
    }
	
	public static BigInteger chineseRemainder(BigInteger a[], BigInteger m[]) {
        BigInteger prod = BigInteger.ONE;
        BigInteger sum = BigInteger.ZERO;

        for (BigInteger n1 : m) {
            prod = prod.multiply(n1);
        }

        for (int i = 0; i < m.length; i++) {
            BigInteger p = prod.divide(m[i]);
            sum = sum.add(a[i].multiply(moduloInverse(p, m[i])).multiply(p));
        }
        
        return sum.remainder(prod);
    }
	
	public static void main(String[] args) {
		ExtendedEuclidean extEud = new ExtendedEuclidean();
		BigInteger[] arr = new BigInteger[6];
		arr[0] = BigInteger.valueOf(4);
		arr[1] = BigInteger.valueOf(5);
		arr[2] = BigInteger.valueOf(600);
		arr[3] = new BigInteger("1000000000000");
		arr[4] = new BigInteger("10000000000000000000000000");
		arr[5] = new BigInteger("100132121321312312321321434254243243242532");
		
		BigInteger[] arr1 = new BigInteger[6];
		arr1[0] = BigInteger.valueOf(2);
		arr1[1] = BigInteger.valueOf(3);
		arr1[2] = BigInteger.valueOf(7);
		arr1[3] = BigInteger.valueOf(11);
		arr1[4] = BigInteger.valueOf(13);
		arr1[5] = BigInteger.valueOf(17);
		
		System.out.println(extEud.chineseRemainder(arr, arr1));
		
		
	}
}
