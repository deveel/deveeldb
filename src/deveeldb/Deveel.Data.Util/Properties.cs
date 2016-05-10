// 
//  Copyright 2010-2016 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//


using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Deveel.Data.Util {
	class Properties : Dictionary<string, string> {

		/// <summary>
		/// Creates a new empty property list with no default values.
		/// </summary>
		public Properties() {
		}

		public object SetProperty(String key, String value) {
			return this[key] = value;
		}

		///<summary>
		///</summary>
		///<param name="inStream"></param>
		public void Load(Stream inStream) {
			// The spec says that the file must be encoded using ISO-8859-1.
			StreamReader reader = new StreamReader(inStream, Encoding.GetEncoding("ISO-8859-1"));
			String line;

			while ((line = reader.ReadLine()) != null) {
				char c = '\0';
				int pos = 0;
				// Leading whitespaces must be deleted first.
				while (pos < line.Length
					   && Char.IsWhiteSpace(c = line[pos]))
					pos++;

				// If empty line or begins with a comment character, skip this line.
				if ((line.Length - pos) == 0
				|| line[pos] == '#' || line[pos] == '!')
					continue;

				// The characters up to the next Whitespace, ':', or '='
				// describe the key.  But look for escape sequences.
				// Try to short-circuit when there is no escape char.
				int start = pos;
				bool needsEscape = line.IndexOf('\\', pos) != -1;
				StringBuilder key = needsEscape ? new StringBuilder() : null;
				while (pos < line.Length
					   && !Char.IsWhiteSpace(c = line[pos++])
					   && c != '=' && c != ':') {
					if (needsEscape && c == '\\') {
						if (pos == line.Length) {
							// The line continues on the next line.  If there
							// is no next line, just treat it as a key with an
							// empty value.
							line = reader.ReadLine();
							if (line == null)
								line = "";
							pos = 0;
							while (pos < line.Length
								   && Char.IsWhiteSpace(c = line[pos]))
								pos++;
						} else {
							c = line[pos++];
							switch (c) {
								case 'n':
									key.Append('\n');
									break;
								case 't':
									key.Append('\t');
									break;
								case 'r':
									key.Append('\r');
									break;
								case 'u':
									if (pos + 4 <= line.Length) {
										char uni = (char)Convert.ToInt32(line.Substring(pos, 4), 16);
										key.Append(uni);
										pos += 4;
									}        // else throw exception?
									break;
								default:
									key.Append(c);
									break;
							}
						}
					} else if (needsEscape)
						key.Append(c);
				}

				bool isDelim = (c == ':' || c == '=');

				String keyString;
				if (needsEscape)
					keyString = key.ToString();
				else if (isDelim || Char.IsWhiteSpace(c))
					keyString = line.Substring(start, (pos - 1) - start);
				else
					keyString = line.Substring(start, pos - start);

				while (pos < line.Length
					   && Char.IsWhiteSpace(c = line[pos]))
					pos++;

				if (!isDelim && (c == ':' || c == '=')) {
					pos++;
					while (pos < line.Length
						   && Char.IsWhiteSpace(c = line[pos]))
						pos++;
				}

				// Short-circuit if no escape chars found.
				if (!needsEscape) {
					this[keyString] = line.Substring(pos);
					continue;
				}

				// Escape char found so iterate through the rest of the line.
				StringBuilder element = new StringBuilder(line.Length - pos);
				while (pos < line.Length) {
					c = line[pos++];
					if (c == '\\') {
						if (pos == line.Length) {
							// The line continues on the next line.
							line = reader.ReadLine();

							// We might have seen a backslash at the end of
							// the file.  The JDK ignores the backslash in
							// this case, so we follow for compatibility.
							if (line == null)
								break;

							pos = 0;
							while (pos < line.Length
								   && Char.IsWhiteSpace(c = line[pos]))
								pos++;
							element.EnsureCapacity(line.Length - pos + element.Length);
						} else {
							c = line[pos++];
							switch (c) {
								case 'n':
									element.Append('\n');
									break;
								case 't':
									element.Append('\t');
									break;
								case 'r':
									element.Append('\r');
									break;
								case 'u':
									if (pos + 4 <= line.Length) {
										char uni = (char)Convert.ToInt32(line.Substring(pos, 4), 16);
										element.Append(uni);
										pos += 4;
									}        // else throw exception?
									break;
								default:
									element.Append(c);
									break;
							}
						}
					} else
						element.Append(c);
				}
				this[keyString] = element.ToString();
			}
		}

		///<summary>
		/// Writes the key/value pairs to the given output stream, in a format
		/// suitable for <see cref="Load"/>.
		///</summary>
		///<param name="output">The output stream.</param>
		///<param name="header">The header written in the first line, may be null.</param>
		/// <remarks>
		/// If header is not null, this method writes a comment containing
		/// the header as first line to the stream.  The next line (or first
		/// line if header is null) contains a comment with the current date.
		/// Afterwards the key/value pairs are written to the stream in the
		/// following format.
		/// <para>
		/// Each line has the form <c>key = value</c>. Newlines, Returns 
		/// and tabs are written as <c>\n,\t,\r</c> resp.
		/// The characters <c>\, !, #, =</c> and <c>:</c> are preceeded by 
		/// a backslash.  Spaces are preceded with a backslash, if and only 
		/// if they are at the beginning of the key.  Characters that are not 
		/// in the ascii range 33 to 127 are written in the <c>\</c><c>u</c>xxxx 
		/// Form.
		/// </para>
		/// <para>
		/// Following the listing, the output stream is flushed but left open.
		/// </para>
		/// </remarks>
		/// <exception cref="InvalidCastException">
		/// If this property contains any key or value that isn't a string.
		/// </exception>
		/// <exception cref="IOException">
		/// If writing to the stream fails.
		/// </exception>
		/// <exception cref="ArgumentNullException">
		/// If <paramref name="output"/> is null.
		/// </exception>
		public void Store(Stream output, String header) {
			// The spec says that the file must be encoded using ISO-8859-1.
			StreamWriter writer = new StreamWriter(output, Encoding.GetEncoding("ISO-8859-1"));
			if (header != null)
				writer.WriteLine("#" + header);
			writer.WriteLine("#" + DateTime.Now);

			StringBuilder s = new StringBuilder(); // Reuse the same buffer.
			foreach (var entry in this) {
				FormatForOutput((String)entry.Key, s, true);
				s.Append('=');
				FormatForOutput((String)entry.Value, s, false);
				writer.WriteLine(s);
			}

			writer.Flush();
		}

		/// <summary>
		/// Formats a key or value for output in a properties file.
		/// </summary>
		/// <param name="str">The string to format.</param>
		/// <param name="buffer">The buffer to add it to.</param>
		/// <param name="key">True if all ' ' must be escaped for the key, false if only 
		/// leading spaces must be escaped for the value</param>
		/// <remarks>
		/// See <see cref="Store"/> for a description of the format.
		/// </remarks>
		private static void FormatForOutput(String str, StringBuilder buffer, bool key) {
			if (key) {
				buffer.Length = 0;
				buffer.EnsureCapacity(str.Length);
			} else
				buffer.EnsureCapacity(buffer.Length + str.Length);
			bool head = true;
			int size = str.Length;
			for (int i = 0; i < size; i++) {
				char c = str[i];
				switch (c) {
					case '\n':
						buffer.Append("\\n");
						break;
					case '\r':
						buffer.Append("\\r");
						break;
					case '\t':
						buffer.Append("\\t");
						break;
					case ' ':
						buffer.Append(head ? "\\ " : " ");
						break;
					case '\\':
					case '!':
					case '#':
					case '=':
					case ':':
						buffer.Append('\\').Append(c);
						break;
					default:
						if (c < ' ' || c > '~') {
							String hex = ((int)c).ToString("{0:x4}");
							buffer.Append("\\u0000".Substring(0, 6 - hex.Length));
							buffer.Append(hex);
						} else
							buffer.Append(c);
						break;
				}
				if (c != ' ')
					head = key;
			}
		}
	}
}