const BUFFER_LEN = 8*1024;
const EOF = 0;
const ASSERTIONS_ENABLED = false;

function assert(expr: unknown): asserts expr
{	if (ASSERTIONS_ENABLED && !expr)
	{	throw new Error('Assertion failed');
	}
}

/**	Decode bytes with given TextDecoder.
	If the encoding is 'utf-8', and bytes has incomplete char at the end, it will be excluded from decoding.
 **/
function decode(bytes: Uint8Array, decoder: TextDecoder): string
{	if (decoder.encoding == 'utf-8')
	{	// remove incomplete char from the end
		let end = bytes.length;
		let b;
		while (((b = bytes[--end]) & 0xC0) == 0xC0) {}
		let char_len = bytes.length - end;
		if (char_len>1 && end>=0)
		{	if ((b&0xE0)==0xC0 && char_len<2 || (b&0xF0)==0xE0 && char_len<3 || (b&0xF8)==0xF0 && char_len<4)
			{	// the last char is cut
				bytes = bytes.subarray(0, end);
			}
		}
	}
	return decoder.decode(bytes);
}

function arraySearchBinary(needle: any, haystack: any[])
{	let from = 0; // inclusive
	let to = haystack.length - 1; // inclusive
	let mid;
	let c;
	while (from <= to)
	{	mid = from + ((to - from) >> 1);
		c = haystack[mid]<needle ? -1 : haystack[mid]>needle ? +1 : 0;
		if (c < 0)
		{	from = mid + 1;
		}
		else if (c > 0)
		{	to = mid - 1;
		}
		else
		{	return mid;
		}
	}
	return -(from + 1); // must place before -(from + 1)
}

/**	Allows to read/write CSV records from/to file.
 **/
export class CsvFile
{	private buffer = new Uint8Array(BUFFER_LEN); // For buffered read. Only range between buffer_pos..buffer_end_pos is set.
	private buffer_pos = 0;
	private buffer_end_pos = 0;
	private file_offset = 0; // Initially 0, which may be wrong if this object was created with file handler that pointed not to the beginning of file. But this is OK, because i use this variable only for record index. The index is initially disabled. The function that enables it is seekRecord(), and this function updates file_offset with the correct value, and it becomes valid since then.
	private n_record = 0; // Valid if index was enabled.
	private index: number[] = []; // Record index. Zero-length means index disabled. Format: index[n_record] = file_offset.

	/**	Header record. Usually it's set by readHeader(). You can modify it.
	 **/
	public header: string[] = [];

	/**	File is assumed to be opened in read and/or write mode, but not append.
		File pointer is not required to be at the beginning.
		Technically all the public fields, like delimiter, limitField, etc. can be modified at any time, and the object will continue reading/writing with the new values.

		`endl` is used only for writing records. Reader understands any combination of \r and \n.
	 **/
	constructor
	(	private file: Deno.File,
		public delimiter = ',',
		public enclosure = '"',
		public endl = '\n',
		public limitField = 0x7FFFFFFF,
		public limitRecord = 0x7FFFFFFF,
		public decoder = new TextDecoder
	){}

	private async readByte(): Promise<number>
	{	if (this.buffer_pos >= this.buffer_end_pos) // if buffer is empty
		{	this.buffer_pos = 0;
			let read = (await this.file.read(this.buffer)) ?? 0;
			this.buffer_end_pos = read;
			this.file_offset += read;
			if (read == 0)
			{	return EOF;
			}
		}
		return this.buffer[this.buffer_pos++];
	}

	private readByteSync(): number
	{	if (this.buffer_pos >= this.buffer_end_pos) // if buffer is empty
		{	this.buffer_pos = 0;
			let read = this.file.readSync(this.buffer) ?? 0;
			this.buffer_end_pos = read;
			this.file_offset += read;
			if (read == 0)
			{	return EOF;
			}
		}
		return this.buffer[this.buffer_pos++];
	}

	async seek(offset: number, whence=Deno.SeekMode.Start): Promise<number>
	{	let correction = this.wantSeek(offset, whence);
		offset = correction>=0 ? 0 : offset+correction+1; // correct according to buffered bytes
		let new_offset = (await this.file.seek(offset, whence)) - (correction>=0 ? correction : 0);
		this.updateAfterSeek(new_offset);
		return new_offset;
	}

	seekSync(offset: number, whence=Deno.SeekMode.Start): number
	{	let correction = this.wantSeek(offset, whence);
		offset = correction>=0 ? 0 : offset+correction+1; // correct according to buffered bytes
		let new_offset = this.file.seekSync(offset, whence) - (correction>=0 ? correction : 0);
		this.updateAfterSeek(new_offset);
		return new_offset;
	}

	private wantSeek(offset: number, whence: Deno.SeekMode): number
	{	let buffered = this.buffer_end_pos - this.buffer_pos;
		if (buffered != 0)
		{	if (whence == Deno.SeekMode.Current)
			{	if (offset>=0 && offset<=buffered)
				{	this.buffer_pos += offset;
					return buffered - offset; // return correction >= 0 (buffer not dropped)
				}
			}
			this.buffer_pos = this.buffer_end_pos; // drop buffer
		}
		return whence==Deno.SeekMode.Current ? -buffered-1 : -1; // return correction < 0 (buffer dropped)
	}

	private updateAfterSeek(new_offset: number)
	{	if (this.index.length) // if index is enabled
		{	this.file_offset = new_offset + (this.buffer_end_pos - this.buffer_pos);
			let pos = arraySearchBinary(new_offset, this.index);
			if (pos >= 0)
			{	this.n_record = pos;
			}
			else
			{	// In the middle of record, so next readRecord will read a half record.
				// So disable the index.
				this.index = [];
			}
		}
	}

	/**	Allows you to jump to specific record number.
	 	Current record number can be found like this: `n = await seekRecord(0, Deno.SeekMode.Current)`.
	 	Number of records can be found like this: `length = await seekRecord(0, Deno.SeekMode.End)`. This will also shift the file pointer to the end of file.

		After the first call to this function, this object creates search index, and maintains it while you read or write further records.
		If you are planning to call this function, it's reasonable to create the index immediately after opening the file by calling seekRecord(0).
		The index is internal array of file offsets for records read or written so far.

		Throws Error, if file pointer is currently in the middle of record, and Deno.SeekMode.Current is requested.
		The file pointer can find himself in the middle of record only if you called seek().
	 **/
	async seekRecord(n_record: number, whence=Deno.SeekMode.Start): Promise<number>
	{	if (!this.index.length) // if index is not enabled
		{	this.index[0] = 0; // enable index
			let cur_rec_file_offset = this.file_offset - (this.buffer_end_pos - this.buffer_pos);
			this.file_offset = 0;
			await this.file.seek(0, Deno.SeekMode.Start);
			this.buffer_pos = this.buffer_end_pos; // drop buffer
			this.n_record = 0;
			if (whence == Deno.SeekMode.Current)
			{	while (true)
				{	let rec_file_offset = this.file_offset - (this.buffer_end_pos - this.buffer_pos);
					if (rec_file_offset == cur_rec_file_offset)
					{	break;
					}
					if (rec_file_offset > cur_rec_file_offset)
					{	throw new Error('In the middle of record');
					}
					if (!await this.skipRecord())
					{	return this.n_record;
					}
				}
			}
		}
		if (whence == Deno.SeekMode.Current)
		{	// correct n_record
			n_record += this.n_record;
		}
		else if (whence == Deno.SeekMode.End)
		{	// go to last known record
			this.file_offset = this.index[this.index.length - 1];
			await this.file.seek(this.file_offset, Deno.SeekMode.Start);
			this.buffer_pos = this.buffer_end_pos; // drop buffer
			this.n_record = this.index.length - 1;
			// scroll till the end
			while (await this.skipRecord()) {}
			// correct n_record
			n_record += this.n_record;
		}
		if (n_record < this.index.length)
		{	if (n_record != this.n_record)
			{	if (n_record < 0)
				{	n_record = 0;
				}
				this.file_offset = this.index[n_record];
				await this.file.seek(this.file_offset, Deno.SeekMode.Start);
				this.buffer_pos = this.buffer_end_pos; // drop buffer
				this.n_record = n_record;
			}
		}
		else if (whence != Deno.SeekMode.End)
		{	// go to last known record
			this.file_offset = this.index[this.index.length - 1];
			await this.file.seek(this.file_offset, Deno.SeekMode.Start);
			this.buffer_pos = this.buffer_end_pos; // drop buffer
			this.n_record = this.index.length - 1;
			while (this.n_record < n_record)
			{	if (!await this.skipRecord())
				{	break;
				}
			}
		}
		return this.n_record;
	}

	seekRecordSync(n_record: number, whence=Deno.SeekMode.Start): number
	{	if (!this.index.length) // if index is not enabled
		{	this.index[0] = 0; // enable index
			let cur_rec_file_offset = this.file_offset - (this.buffer_end_pos - this.buffer_pos);
			this.file_offset = 0;
			this.file.seekSync(0, Deno.SeekMode.Start);
			this.buffer_pos = this.buffer_end_pos; // drop buffer
			this.n_record = 0;
			if (whence == Deno.SeekMode.Current)
			{	while (true)
				{	let rec_file_offset = this.file_offset - (this.buffer_end_pos - this.buffer_pos);
					if (rec_file_offset == cur_rec_file_offset)
					{	break;
					}
					if (rec_file_offset > cur_rec_file_offset)
					{	throw new Error('In the middle of record');
					}
					if (!this.skipRecordSync())
					{	return this.n_record;
					}
				}
			}
		}
		if (whence == Deno.SeekMode.Current)
		{	// correct n_record
			n_record += this.n_record;
		}
		else if (whence == Deno.SeekMode.End)
		{	// go to last known record
			this.file_offset = this.index[this.index.length - 1];
			this.file.seekSync(this.file_offset, Deno.SeekMode.Start);
			this.buffer_pos = this.buffer_end_pos; // drop buffer
			this.n_record = this.index.length - 1;
			// scroll till the end
			while (this.skipRecordSync()) {}
			// correct n_record
			n_record += this.n_record;
		}
		if (n_record < this.index.length)
		{	if (n_record != this.n_record)
			{	if (n_record < 0)
				{	n_record = 0;
				}
				this.file_offset = this.index[n_record];
				this.file.seekSync(this.file_offset, Deno.SeekMode.Start);
				this.buffer_pos = this.buffer_end_pos; // drop buffer
				this.n_record = n_record;
			}
		}
		else if (whence != Deno.SeekMode.End)
		{	// go to last known record
			this.file_offset = this.index[this.index.length - 1];
			this.file.seekSync(this.file_offset, Deno.SeekMode.Start);
			this.buffer_pos = this.buffer_end_pos; // drop buffer
			this.n_record = this.index.length - 1;
			while (this.n_record < n_record)
			{	if (!this.skipRecordSync())
				{	break;
				}
			}
		}
		return this.n_record;
	}

	private async skipRecord(): Promise<boolean>
	{	let delimiter_code = this.delimiter.charCodeAt(0);
		let enclosure_code = this.enclosure.charCodeAt(0);
		let n_fields = 0;
		let has_field = false;
		let c;
		while (true)
		{	c = await this.readByte();
			while (c == enclosure_code)
			{	while ((c = await this.readByte()) != EOF && c != enclosure_code)
				{	has_field = true;
				}
				if (c != EOF)
				{	// assume: at '"'
					c = await this.readByte(); // read next char
					if (c == enclosure_code) // double '"'
					{	has_field = true;
					}
				}
			}
			if (c==EOF || c==13 || c==10)
			{	if (has_field || n_fields || c==EOF)
				{	break;
				}
				// assume: sequence of "\r" and/or "\n" chars, like "\n\n\n" or "\r\n"
			}
			else if (c == delimiter_code)
			{	n_fields++;
			}
			else
			{	has_field = true;
			}
		}
		n_fields++;
		if (c==EOF && n_fields==1 && !has_field)
		{	return false;
		}
		if (this.index.length) // if index is enabled
		{	this.index[++this.n_record] = this.file_offset - (this.buffer_end_pos - this.buffer_pos);
		}
		return true;
	}

	private skipRecordSync(): boolean
	{	let delimiter_code = this.delimiter.charCodeAt(0);
		let enclosure_code = this.enclosure.charCodeAt(0);
		let n_fields = 0;
		let has_field = false;
		let c;
		while (true)
		{	c = this.readByteSync();
			while (c == enclosure_code)
			{	while ((c = this.readByteSync()) != EOF && c != enclosure_code)
				{	has_field = true;
				}
				if (c != EOF)
				{	// assume: at '"'
					c = this.readByteSync(); // read next char
					if (c == enclosure_code) // double '"'
					{	has_field = true;
					}
				}
			}
			if (c==EOF || c==13 || c==10)
			{	if (has_field || n_fields || c==EOF)
				{	break;
				}
				// assume: sequence of "\r" and/or "\n" chars, like "\n\n\n" or "\r\n"
			}
			else if (c == delimiter_code)
			{	n_fields++;
			}
			else
			{	has_field = true;
			}
		}
		n_fields++;
		if (c==EOF && n_fields==1 && !has_field)
		{	return false;
		}
		if (this.index.length) // if index is enabled
		{	this.index[++this.n_record] = this.file_offset - (this.buffer_end_pos - this.buffer_pos);
		}
		return true;
	}

	async readRecord(): Promise<string[] | null>
	{	let delimiter_code = this.delimiter.charCodeAt(0);
		let enclosure_code = this.enclosure.charCodeAt(0);
		let record: string[] = [];
		let field = [];
		let read_chars_record = 0;
		let c;
		while (true)
		{	c = await this.readByte();
			while (c == enclosure_code)
			{	while ((c = await this.readByte()) != EOF && c != enclosure_code)
				{	if (field.length<this.limitField && ++read_chars_record<this.limitRecord)
					{	field[field.length] = c;
					}
				}
				if (c != EOF)
				{	// assume: at '"'
					c = await this.readByte(); // read next char
					if (c == enclosure_code) // double '"'
					{	if (field.length<this.limitField && ++read_chars_record<this.limitRecord)
						{	field[field.length] = c;
						}
					}
				}
			}
			if (c==EOF || c==13 || c==10)
			{	if (field.length || record.length || c==EOF)
				{	break;
				}
				// assume: sequence of "\r" and/or "\n" chars, like "\n\n\n" or "\r\n"
			}
			else if (c == delimiter_code)
			{	record[record.length] = decode(new Uint8Array(field), this.decoder);
				field.length = 0;
			}
			else
			{	if (field.length<this.limitField && ++read_chars_record<this.limitRecord)
				{	field[field.length] = c;
				}
			}
		}
		record[record.length] = decode(new Uint8Array(field), this.decoder);
		if (c==EOF && record.length==1 && !field.length)
		{	return null;
		}
		if (this.index.length) // if index is enabled
		{	this.index[++this.n_record] = this.file_offset - (this.buffer_end_pos - this.buffer_pos);
		}
		return record;
	}

	readRecordSync(): string[] | null
	{	let delimiter_code = this.delimiter.charCodeAt(0);
		let enclosure_code = this.enclosure.charCodeAt(0);
		let record: string[] = [];
		let field = [];
		let read_chars_record = 0;
		let c;
		while (true)
		{	c = this.readByteSync();
			while (c == enclosure_code)
			{	while ((c = this.readByteSync()) != EOF && c != enclosure_code)
				{	if (field.length<this.limitField && ++read_chars_record<this.limitRecord)
					{	field[field.length] = c;
					}
				}
				if (c != EOF)
				{	// assume: at '"'
					c = this.readByteSync(); // read next char
					if (c == enclosure_code) // double '"'
					{	if (field.length<this.limitField && ++read_chars_record<this.limitRecord)
						{	field[field.length] = c;
						}
					}
				}
			}
			if (c==EOF || c==13 || c==10)
			{	if (field.length || record.length || c==EOF)
				{	break;
				}
				// assume: sequence of "\r" and/or "\n" chars, like "\n\n\n" or "\r\n"
			}
			else if (c == delimiter_code)
			{	record[record.length] = decode(new Uint8Array(field), this.decoder);
				field.length = 0;
			}
			else
			{	if (field.length<this.limitField && ++read_chars_record<this.limitRecord)
				{	field[field.length] = c;
				}
			}
		}
		record[record.length] = decode(new Uint8Array(field), this.decoder);
		if (c==EOF && record.length==1 && !field.length)
		{	return null;
		}
		if (this.index.length) // if index is enabled
		{	this.index[++this.n_record] = this.file_offset - (this.buffer_end_pos - this.buffer_pos);
		}
		return record;
	}

	/**	Read header record - usually the first line in file, that contains column titles.
		Then you can readMap().
	 **/
	async readHeader(): Promise<string[]>
	{	this.header = (await this.readRecord()) ?? [];
		return this.header;
	}

	readHeaderSync(): string[]
	{	this.header = this.readRecordSync() ?? [];
		return this.header;
	}

	private recordToMap(record: string[]|null)
	{	if (!record)
		{	return null;
		}
		let {header} = this;
		let map = new Map;
		for (let i=0, i_end=header.length; i<i_end; i++)
		{	map.set(header[i], record[i] ?? '');
		}
		return map;
	}

	async readMap(): Promise<Map<string, string> | null>
	{	return this.recordToMap(await this.readRecord());
	}

	readMapSync(): Map<string, string> | null
	{	return this.recordToMap(this.readRecordSync());
	}

	private serializeRecord(record: string[]): Uint8Array
	{	let str = '';
		for (let i=0, i_end=record.length-1; i<=i_end; i++)
		{	let field = record[i];
			if (field.indexOf('\r')==-1 && field.indexOf('\n')==-1 && field.indexOf(this.delimiter)==-1 && field.indexOf(this.enclosure)==-1)
			{	str += field;
			}
			else
			{	str += this.enclosure;
				str += field.replaceAll(this.enclosure, this.enclosure+this.enclosure);
				str += this.enclosure;
			}
			str += i==i_end ? this.endl : this.delimiter;
		}
		return new TextEncoder().encode(str);
	}

	async writeRecord(record: string[]): Promise<number>
	{	let buffered = this.buffer_end_pos - this.buffer_pos;
		if (buffered != 0)
		{	await this.file.seek(-buffered, Deno.SeekMode.Current);
			this.buffer_pos = this.buffer_end_pos; // drop buffer
			this.file_offset -= buffered;
		}
		let bytes = this.serializeRecord(record);
		let n_bytes = bytes.length;
		while (bytes.length)
		{	let written = await this.file.write(bytes);
			bytes = bytes.subarray(written);
		}
		if (this.index.length) // if index is enabled
		{	this.file_offset += n_bytes;
			this.index[++this.n_record] = this.file_offset;
			this.index.length = this.n_record+1; // cut tail
		}
		return n_bytes;
	}

	writeRecordSync(record: string[]): number
	{	let buffered = this.buffer_end_pos - this.buffer_pos;
		if (buffered != 0)
		{	this.file.seekSync(-buffered, Deno.SeekMode.Current);
			this.buffer_pos = this.buffer_end_pos; // drop buffer
			this.file_offset -= buffered;
		}
		let bytes = this.serializeRecord(record);
		let n_bytes = bytes.length;
		while (bytes.length)
		{	let written = this.file.writeSync(bytes);
			bytes = bytes.subarray(written);
		}
		if (this.index.length) // if index is enabled
		{	this.file_offset += n_bytes;
			this.index[++this.n_record] = this.file_offset;
			this.index.length = this.n_record+1; // cut tail
		}
		return n_bytes;
	}

	close()
	{	this.file.close();
	}

	async *[Symbol.asyncIterator](): AsyncGenerator<string[]>
	{	let record;
		while ((record = await this.readRecord()))
		{	yield record;
		}
	}

	*[Symbol.iterator](): Generator<string[]>
	{	let record;
		while ((record = this.readRecordSync()))
		{	yield record;
		}
	}

	maps(): CsvFileMapsIterator
	{	return new CsvFileMapsIterator(this);
	}
}

class CsvFileMapsIterator
{	constructor
	(	private csv: CsvFile
	){}

	async *[Symbol.asyncIterator](): AsyncGenerator<Map<string, string>>
	{	let record;
		while ((record = await this.csv.readMap()))
		{	yield record;
		}
	}

	*[Symbol.iterator](): Generator<Map<string, string>>
	{	let record;
		while ((record = this.csv.readMapSync()))
		{	yield record;
		}
	}
}
