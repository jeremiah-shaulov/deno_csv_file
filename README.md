# deno_csv_file
CSV file reader and writer for Deno (seekable, sync/async).

[![deno doc](https://doc.deno.land/badge.svg)](https://doc.deno.land/https/deno.land/x/csv_file/mod.ts)

## Example

```typescript
import {CsvFile} from "https://deno.land/x/csv_file/mod.ts";

// 1. Create a new file "/tmp/example-1.csv"
let csv = new CsvFile(await Deno.open('/tmp/example-1.csv', {read: true, write: true, create: true}));

// 2. Write some records
await csv.writeRecord(['Day', 'Month', 'Year']);
await csv.writeRecord(['1', 'January', '2000']);
await csv.writeRecord(['2', 'January', '2000']);
await csv.writeRecord(['3', 'January', '2000']);

// 3. Scroll to the beginning
await csv.seekRecord(0);

// 4. Read the records back
for await (let record of csv)
{	console.log(record);
}

// 5. Now try another way of reading records. So scroll to the beginning again
await csv.seekRecord(0);

// 6. Read and remember a header row
await csv.readHeader();

// 7. Read Map objects with column names taken from header
for await (let record of csv.maps())
{	console.log(record);
}

// 8. Manually read record no. 2, and it's offset in file

await csv.seekRecord(2);
let file_offset = await csv.seek(0, Deno.SeekMode.Current);
let record = await csv.readRecord();
console.log(`Record no. 2 at ${file_offset}:`, record);

// 9. Done
csv.close();
```

Run like this:

```bash
deno run --allow-read --allow-write test.ts
```
