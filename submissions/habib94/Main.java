

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;


public class Main {
    //private static final String FILE = "input_o.txt";
    private static final String FILE = "/home/habib/Downloads/input.txt";
    //private static final String FILE = "input_from_git.txt";
    private static final int MIN_TEMP = -999;
    private static final int MAX_TEMP = 999;
    private static final int MAX_NAME_LENGTH = 100;
    private static final int MAX_CITIES = 101;
    private static final int SEGMENT_SIZE = 1 << 21;
    private static final int HASH_TABLE_SIZE = 1 << 17;
    public static final long DELIMITER = 0x2C2C2C2C2C2C2C2CL;
    public static final char DELIMEETR_CHAR = ',';

    public static void main(String[] args) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        process(args);
        long end = System.currentTimeMillis();
        System.out.println("end = " + (end - start));
    }

    private static List<MappedByteBuffer> buildChunks() throws IOException {
        var file = new RandomAccessFile(FILE, "r");
        var fileChannel = file.getChannel();
        var fileSize = fileChannel.size();
        var chunkSize = Math.min(Integer.MAX_VALUE - 512, fileSize / Runtime.getRuntime().availableProcessors());
        if (chunkSize <= 0) {
            chunkSize = fileSize;
        }
        var chunks = new ArrayList<MappedByteBuffer>((int) (fileSize / chunkSize) + 1);
        var start = 0L;
        while (start < fileSize) {
            var pos = start + chunkSize;
            if (pos < fileSize) {
                file.seek(pos);
                while (file.read() != '\n') {
                    pos += 1;
                }
                pos += 1;
            } else {
                pos = fileSize;
            }

            var buf = fileChannel.map(FileChannel.MapMode.READ_ONLY, start, pos - start);
            buf.order(ByteOrder.nativeOrder());
            chunks.add(buf);
            start = pos;
        }
        return chunks;
    }

    public static void process(String[] args) throws IOException, InterruptedException {
        List<MappedByteBuffer> chunks = buildChunks();
        Thread[] threads = new Thread[chunks.size()];
        List<CityResult>[] cityResuls = new List[chunks.size()];
        List<ProductResult>[] productsResuls = new List[chunks.size()];
        for (int i = 0; i < chunks.size(); ++i) {
            final int index = i;
            threads[i] = new Thread(() -> {
                List<CityResult> cityResults = new ArrayList<>(MAX_CITIES);
                List<ProductResult> productResults = new ArrayList<>(MAX_CITIES);
                parseLoop(chunks.get(index), cityResults, productResults);
                cityResuls[index] = cityResults;
                productsResuls[index] = productResults;
            });
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }

        CityResult minSumCity = Arrays.stream(cityResuls).parallel().flatMap(Collection::stream).collect(Collectors.toMap(
                        Result::calcName,
                        Function.identity(),
                        (cityResult, cityResult2) -> {
                            cityResult.accumulate(cityResult2);
                            return cityResult;
                        }
                )).entrySet()
                .stream()
                .parallel()
                .min(Comparator.comparing(entry -> entry.getValue().sum))
                .get()
                .getValue();
        // Final output.

        List<ProductResult> min5Products = Arrays.stream(productsResuls).parallel().flatMap(Collection::stream).collect(Collectors.toMap(
                        Result::calcName,
                        Function.identity(),
                        (cityResult, cityResult2) -> {
                            cityResult.accumulate(cityResult2);
                            return cityResult;
                        }
                )).entrySet()
                .stream()
                .parallel()
                .sorted(Comparator.<Map.Entry<String, ProductResult>, Long>comparing(entry -> entry.getValue().min).thenComparing(enty -> enty.getKey()))
                .limit(5)
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());

        System.out.println(minSumCity);
        System.out.println("MIN 5");
        System.out.println(min5Products);

    }


    private static TreeMap<String, CityResult> accumulateResults(List<CityResult>[] allResults) {
        TreeMap<String, CityResult> result = new TreeMap<>();
        for (List<CityResult> cityResultArr : allResults) {
            for (CityResult r : cityResultArr) {
                CityResult current = result.putIfAbsent(r.calcName(), r);
                if (current != null) {
                    current.accumulate(r);
                }
            }
        }
        return result;
    }

    private static void parseLoop(MappedByteBuffer mappedByteBuffer, List<CityResult> collectedCityResults, List<ProductResult> collectedProductResults) {
        CityResult[] cityResults = new CityResult[HASH_TABLE_SIZE];
        ProductResult[] productResults = new ProductResult[HASH_TABLE_SIZE];

        long segmentEnd = nextNewLine(mappedByteBuffer, (long) mappedByteBuffer.limit() - 1);
        long segmentStart = mappedByteBuffer.position();

        long dist = (segmentEnd - segmentStart) / 3;
        long midPoint1 = nextNewLine(mappedByteBuffer, segmentStart + dist);
        long midPoint2 = nextNewLine(mappedByteBuffer, segmentStart + dist + dist);

        Scanner scanner1 = new Scanner(mappedByteBuffer, segmentStart, midPoint1);
        Scanner scanner2 = new Scanner(mappedByteBuffer, midPoint1 + 1, midPoint2);
        Scanner scanner3 = new Scanner(mappedByteBuffer, midPoint2 + 1, segmentEnd);
        while (true) {
            if (!scanner1.hasNext()) {
                break;
            }
            if (!scanner2.hasNext()) {
                break;
            }
            if (!scanner3.hasNext()) {
                break;
            }
            parseNextUsingScanner(collectedCityResults, scanner1, cityResults, collectedProductResults, productResults, mappedByteBuffer);
            parseNextUsingScanner(collectedCityResults, scanner2, cityResults, collectedProductResults, productResults, mappedByteBuffer);
            parseNextUsingScanner(collectedCityResults, scanner3, cityResults, collectedProductResults, productResults, mappedByteBuffer);
        }

        parseWhileHasNextUsingScanner(collectedCityResults, scanner1, cityResults, collectedProductResults, productResults, mappedByteBuffer);
        parseWhileHasNextUsingScanner(collectedCityResults, scanner2, cityResults, collectedProductResults, productResults, mappedByteBuffer);
        parseWhileHasNextUsingScanner(collectedCityResults, scanner3, cityResults, collectedProductResults, productResults, mappedByteBuffer);

    }

    private static void parseWhileHasNextUsingScanner(List<CityResult> collectedCityResults, Scanner scanner, CityResult[] cityResults, List<ProductResult> collectedProductResults, ProductResult[] productResults, MappedByteBuffer mappedByteBuffer) {
        while (scanner.hasNext()) {
            parseNextUsingScanner(collectedCityResults, scanner, cityResults, collectedProductResults, productResults, mappedByteBuffer);
        }
    }

    private static void parseNextUsingScanner(List<CityResult> collectedCityResults,
                                              Scanner scanner,
                                              CityResult[] cityResults,
                                              List<ProductResult> collectedProductResults,
                                              ProductResult[] productResults, MappedByteBuffer mappedByteBuffer) {
        BytesName cityBytesName = BytesName.parse(scanner);
        CityResult cityResult = findResult(cityBytesName, scanner, cityResults, (nameAddress, tableIndex, nameLength) ->
                newCityEntry(cityResults, nameAddress, tableIndex, nameLength, scanner, collectedCityResults, mappedByteBuffer)
        );
        scanner.add(1);
        BytesName productBytesName = BytesName.parse(scanner);
        ProductResult productResult = findResult(productBytesName, scanner, productResults, (nameAddress, tableIndex, nameLength) ->
                newProductEntry(productResults, nameAddress, tableIndex, nameLength, scanner, collectedProductResults, mappedByteBuffer)
        );
        long number = scanNumber(scanner);
        cityResult.sum += number;
        if (productResult.min > number) {
            productResult.min = number;
        }
    }

    private static final long[] MASK1 = new long[]{0xFFL, 0xFFFFL, 0xFFFFFFL, 0xFFFFFFFFL, 0xFFFFFFFFFFL, 0xFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL,
            0xFFFFFFFFFFFFFFFFL};
    private static final long[] MASK2 = new long[]{0x00L, 0x00L, 0x00L, 0x00L, 0x00L, 0x00L, 0x00L, 0x00L, 0xFFFFFFFFFFFFFFFFL};

    private static final class BytesName {
        long initialWord, initialDelimiterMask, wordB, delimiterMaskB, hash, word, word2, mask;
        int letterCount1, letterCount2;

        private static BytesName parse(Scanner scanner) {
            long word = scanner.getLong();
            long pos = findDelimiter(word);
            long wordB = scanner.getLongAt(scanner.pos() + 8);
            long posB = findDelimiter(wordB);
            return new BytesName(word, pos, wordB, posB);
        }

        public BytesName(long initialWord, long initialDelimiterMask, long wordB, long delimiterMaskB) {
            this.initialWord = initialWord;
            this.initialDelimiterMask = initialDelimiterMask;
            this.wordB = wordB;
            this.delimiterMaskB = delimiterMaskB;

            word = initialWord;
            word2 = wordB;
            long delimiterMask = initialDelimiterMask;
            long delimiterMask2 = delimiterMaskB;
            if ((delimiterMask | delimiterMask2) != 0) {
                letterCount1 = Long.numberOfTrailingZeros(delimiterMask) >>> 3; // value between 1 and 8
                letterCount2 = Long.numberOfTrailingZeros(delimiterMask2) >>> 3; // value between 0 and 8
                mask = MASK2[letterCount1];
                word = word & MASK1[letterCount1];
                word2 = mask & word2 & MASK1[letterCount2];
                hash = word ^ word2;
            }
        }

        public boolean test(long firstNameWord, long secondNameWord) {
            return firstNameWord == word && secondNameWord == word2;
        }

        public long scannerAdd() {
            return letterCount1 + (letterCount2 & mask);
        }
    }


    private static <T extends Result> T findResult(BytesName bytesName,
                                                   Scanner scanner,
                                                   T[] results,
                                                   NewResultFunction newResultFunction) {
        T existingCityResult;
        long word = bytesName.initialWord;
        long delimiterMask = bytesName.initialDelimiterMask;
        long hash;
        long nameAddress = scanner.pos();
        long word2 = bytesName.wordB;
        long delimiterMask2 = bytesName.delimiterMaskB;
        if ((delimiterMask | delimiterMask2) != 0) {
            hash = bytesName.hash;
            existingCityResult = results[hashToIndex(hash)];
            scanner.add(bytesName.scannerAdd());
            if (existingCityResult != null && bytesName.test(existingCityResult.firstNameWord, existingCityResult.secondNameWord)) {
                return existingCityResult;
            }
        } else {
            // Slow-path for when the delimiter could not be found in the first 16 bytes.
            hash = word ^ word2;
            scanner.add(16);
            while (true) {
                word = scanner.getLong();
                delimiterMask = findDelimiter(word);
                if (delimiterMask != 0) {
                    int trailingZeros = Long.numberOfTrailingZeros(delimiterMask);
                    word = (word << (63 - trailingZeros));
                    scanner.add(trailingZeros >>> 3);
                    hash ^= word;
                    break;
                } else {
                    scanner.add(8);
                    hash ^= word;
                }
            }
        }

        // Save length of name for later.
        int nameLength = (int) (scanner.pos() - nameAddress);

        // Final calculation for index into hash table.
        int tableIndex = hashToIndex(hash);
        outer:
        while (true) {
            existingCityResult = results[tableIndex];
            if (existingCityResult == null) {
                existingCityResult = (T) newResultFunction.create(nameAddress, tableIndex, nameLength);
            }
            // Check for collision.
            int i = 0;
            for (; i < nameLength + 1 - 8; i += 8) {
                if (scanner.getLongAt(existingCityResult.nameAddress + i) != scanner.getLongAt(nameAddress + i)) {
                    // Collision error, try next.
                    tableIndex = (tableIndex + 31) & (results.length - 1);
                    continue outer;
                }
            }

            int remainingShift = (64 - ((nameLength + 1 - i) << 3));
            if (((scanner.getLongAt(existingCityResult.nameAddress + i) ^ (scanner.getLongAt(nameAddress + i))) << remainingShift) == 0) {
                break;
            } else {
                // Collision error, try next.
                tableIndex = (tableIndex + 31) & (results.length - 1);
            }
        }
        return existingCityResult;
    }

    private static long nextNewLine(MappedByteBuffer mappedByteBuffer, long prev) {
        while (true) {
            long currentWord = new Scanner(mappedByteBuffer, prev, prev + 8).getLong();
            long input = currentWord ^ 0x0A0A0A0A0A0A0A0AL;
            long pos = (input - 0x0101010101010101L) & ~input & 0x8080808080808080L;
            if (pos != 0) {
                prev += Long.numberOfTrailingZeros(pos) >>> 3;
                break;
            } else {
                prev += 8;
            }
        }
        return prev;
    }

    private static long scanNumber(Scanner scanPtr) {
        long numberWord = scanPtr.getLongAt(scanPtr.pos() + 1);
        int decimalSepPos = Long.numberOfTrailingZeros(~numberWord & 0x10101000L);
        long number = convertIntoNumber(decimalSepPos, numberWord);
        scanPtr.add((decimalSepPos >>> 3) + 4);
        return number;
    }

    private static int hashToIndex(long hash) {
        long hashAsInt = hash ^ (hash >>> 33) ^ (hash >>> 15);
        return (int) (hashAsInt & (HASH_TABLE_SIZE - 1));
    }

    // Special method to convert a number in the ascii number into an int without branches created by Quan Anh Mai.
    private static long convertIntoNumber(int decimalSepPos, long numberWord) {
        int shift = 36 - decimalSepPos;
        // signed is -1 if negative, 0 otherwise
        long signed = (~numberWord << 59) >> 63;
        long designMask = ~(signed & 0xFF);
        // Align the number to a specific position and transform the ascii to digit value
        long digits = ((numberWord & designMask) << shift);
        String string = new String(ByteBuffer.allocate(8).order(ByteOrder.nativeOrder()).putLong(digits).array(), StandardCharsets.UTF_8);
        string = string.split("\n")[0];
        return (long) (Double.parseDouble(string) * 100);
    }

    private static long findDelimiter(long word) {
        long input = word ^ DELIMITER;
        return (input - 0x0101010101010101L) & ~input & 0x8080808080808080L;
    }

    private static CityResult newCityEntry(CityResult[] cityResults, long nameAddress, int hash, int nameLength, Scanner scanner, List<CityResult> collectedCityResults, MappedByteBuffer mappedByteBuffer) {
        CityResult r = new CityResult();
        cityResults[hash] = r;
        fillResult(nameAddress, nameLength, scanner, r, mappedByteBuffer);
        collectedCityResults.add(r);
        return r;
    }

    private static void fillResult(long nameAddress, int nameLength, Scanner scanner, Result r, MappedByteBuffer mappedByteBuffer) {
        int totalLength = nameLength + 1;
        r.mappedByteBuffer = mappedByteBuffer;
        r.firstNameWord = scanner.getLongAt(nameAddress);
        r.secondNameWord = scanner.getLongAt(nameAddress + 8);
        if (totalLength <= 8) {
            r.firstNameWord = r.firstNameWord & MASK1[totalLength - 1];
            r.secondNameWord = 0;
        } else if (totalLength < 16) {
            r.secondNameWord = r.secondNameWord & MASK1[totalLength - 9];
        }
        r.nameAddress = nameAddress;
    }

    private static ProductResult newProductEntry(ProductResult[] productResults, long nameAddress, int hash, int nameLength, Scanner scanner, List<ProductResult> collectedCityResults, MappedByteBuffer mappedByteBuffer) {
        ProductResult r = new ProductResult();
        productResults[hash] = r;
        fillResult(nameAddress, nameLength, scanner, r, mappedByteBuffer);
        collectedCityResults.add(r);
        return r;
    }

    interface NewResultFunction {

        Result create(long nameAddress, int hash, int nameLength);

    }


    private static abstract class Result {
        MappedByteBuffer mappedByteBuffer;
        long firstNameWord, secondNameWord;
        long nameAddress;
        String name;

        public String calcName() {
            if (name == null) {
                Scanner scanner = new Scanner(mappedByteBuffer,nameAddress, nameAddress + MAX_NAME_LENGTH + 1);
                int nameLength = 0;
                while (scanner.getByteAt(nameAddress + nameLength) != DELIMEETR_CHAR) {
                    nameLength++;
                }
                byte[] array = new byte[nameLength];
                for (int i = 0; i < nameLength; ++i) {
                    array[i] = scanner.getByteAt(nameAddress + i);
                }
                name = new String(array, java.nio.charset.StandardCharsets.UTF_8);
            }
            return name;
        }

        protected static double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        abstract void accumulate(Result other);
    }

    private static final class ProductResult extends Result {
        long min;

        private ProductResult() {
            this.min = MAX_TEMP;
        }

        public String toString() {
            return calcName() + " " + round(((double) min) / 100.0);
        }


        public void accumulate(Result other) {
            ProductResult otherProductResult = (ProductResult) other;
            if (min > otherProductResult.min) {
                min = otherProductResult.min;
            }
        }


    }

    private static final class CityResult extends Result {
        long sum;

        private CityResult() {
        }

        public String toString() {
            return calcName() + " " + round(((double) sum) / 100.0);
        }

        public void accumulate(Result other) {
            sum += ((CityResult) other).sum;
        }
    }

    private static final class Scanner {
        private final MappedByteBuffer mappedByteBuffer;
        private long pos;
        private final long end;

        public Scanner(MappedByteBuffer mappedByteBuffer, long start, long end) {
            this.mappedByteBuffer = mappedByteBuffer;
            this.pos = start;
            this.end = end;
        }

        boolean hasNext() {
            return pos < end;
        }

        long pos() {
            return pos;
        }

        void add(long delta) {
            pos += delta;
        }

        long getLong() {
            return getLongAt(pos);
        }

        long getLongAt(long pos) {
            long fileEnd = mappedByteBuffer.limit();
            if (fileEnd - pos <= 0) {
                return Long.MIN_VALUE;
            }
            if (fileEnd - pos >= 8) {
                return mappedByteBuffer.getLong((int) pos);
            }
            if (fileEnd - pos >= 4) {
                return mappedByteBuffer.getInt((int) pos);
            }
            if (fileEnd - pos >= 2) {
                return mappedByteBuffer.getChar((int) pos);
            }
            byte[] dst = new byte[1];
            mappedByteBuffer.get((int) pos, dst, 0, 1);
            return dst[0];
        }

        byte getByteAt(long pos) {
            return (byte) mappedByteBuffer.getChar((int) pos);
        }
    }
}