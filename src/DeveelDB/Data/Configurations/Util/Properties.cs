// 
//  Copyright 2010-2018 Deveel
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

namespace Deveel.Data.Configurations.Util {
	class Properties : Dictionary<string, string> {

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

	}
}