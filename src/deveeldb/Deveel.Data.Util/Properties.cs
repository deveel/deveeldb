// 
//  Copyright 2010-2015 Deveel
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

		protected Properties defaults;

		/// <summary>
		/// Creates a new empty property list with no default values.
		/// </summary>
		public Properties() {
		}

		///<summary>
		/// Create a new empty property list with the specified default values.
		///</summary>
		///<param name="defaults">A <see cref="Properties"/> object containing the 
		/// default values.</param>
		public Properties(Properties defaults) {
			this.defaults = defaults;
		}

		///<summary>
		/// Adds the given key/value pair to this properties.
		///</summary>
		///<param name="key">The key for this property.</param>
		///<param name="value">The value for this property.</param>
		/// <remarks>
		/// This calls the hashtable method put.
		/// </remarks>
		///<returns>
		/// Returns the old value for the given key
		/// </returns>
		/// <seealso cref="GetProperty(string)"/>
		/// <seealso cref="GetProperty(string,string)"/>
		public Object SetProperty(String key, String value) {
			return this[key] = value;
		}

		/**
		 * Reads a property list from an input stream.  The stream should
		 * have the following format: <br>
		 *
		 * An empty line or a line starting with <code>#</code> or
		 * <code>!</code> is ignored.  An backslash (<code>\</code>) at the
		 * end of the line makes the line continueing on the next line
		 * (but make sure there is no whitespace after the backslash).
		 * Otherwise, each line describes a key/value pair. <br>
		 *
		 * The chars up to the first whitespace, = or : are the key.  You
		 * can include this caracters in the key, if you precede them with
		 * a backslash (<code>\</code>). The key is followed by optional
		 * whitespaces, optionally one <code>=</code> or <code>:</code>,
		 * and optionally some more whitespaces.  The rest of the line is
		 * the resource belonging to the key. <br>
		 *
		 * Escape sequences <code>\t, \n, \r, \\, \", \', \!, \#, \ </code>(a
		 * space), and unicode characters with the
		 * <code>\\u</code><em>xxxx</em> notation are detected, and
		 * converted to the corresponding single character. <br>
		 *
		 * 
	  <pre># This is a comment
	  key     = value
	  k\:5      \ a string starting with space and ending with newline\n
	  # This is a multiline specification; note that the value contains
	  # no white space.
	  weekdays: Sunday,Monday,Tuesday,Wednesday,\\
				Thursday,Friday,Saturday
	  # The safest way to include a space at the end of a value:
	  label   = Name:\\u0020</pre>
		 *
		 * @param inStream the input stream
		 * @throws IOException if an error occurred when reading the input
		 * @throws NullPointerException if in is null
		 */
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
		/// Calls <see cref="Store"/> and ignores the <see cref="IOException"/> 
		/// that may be thrown.
		///</summary>
		///<param name="output">The stream to write to.</param>
		///<param name="header">A description of the property list.</param>
		/// <exception cref="InvalidCastException">
		/// If this property contains any key or value that are not strings.
		/// </exception>
		[Obsolete("Use Store(Stream, string) method instead.")]
		public void Save(Stream output, String header) {
			try {
				Store(output, header);
			} catch (IOException) {
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

		///<summary>
		/// Gets the property with the specified key in this property list.
		/// If the key is not found, the default property list is searched.
		/// If the property is not found in the default, null is returned.
		///</summary>
		///<param name="key">The key for this property.</param>
		///<returns>
		/// Returns the value for the given key, or null if not found.
		/// </returns>
		/// <exception cref="InvalidCastException">
		/// If this property contains any key or value that isn't a string.
		/// </exception>
		/// <seealso cref="GetProperty(string,string)"/>
		/// <seealso cref="SetProperty"/>
		public String GetProperty(String key) {
			Properties prop = this;
			// Eliminate tail recursion.
			do {
				String value = (String)prop[key];
				if (value != null)
					return value;
				prop = prop.defaults;
			}
			while (prop != null);
			return null;
		}

		///<summary>
		/// Gets the property with the specified key in this property list.  If
		/// the key is not found, the default property list is searched.  If the
		/// property is not found in the default, the specified defaultValue is
		/// returned.
		///</summary>
		///<param name="key">The key for this property.</param>
		///<param name="defaultValue">A default value.</param>
		///<returns>
		/// Returns the value for the given key.
		/// </returns>
		/// <exception cref="InvalidCastException">
		/// If this property contains any key or value that isn't a string.
		/// </exception>
		/// <seealso cref="SetProperty"/>
		public String GetProperty(String key, String defaultValue) {
			String prop = GetProperty(key);
			if (prop == null)
				prop = defaultValue;
			return prop;
		}

		public ICollection PropertyNames {
			get {
				// We make a new Set that holds all the keys, then return an enumeration
				// for that. This prevents modifications from ruining the enumeration,
				// as well as ignoring duplicates.
				Properties prop = this;
				var s = new List<string>();
				// Eliminate tail recursion.
				do {
					s.AddRange(prop.Keys);
					prop = prop.defaults;
				} while (prop != null);
				return s;
			}
		}

		///<summary>
		/// Prints the key/value pairs to the given print stream.
		///</summary>
		///<param name="output">The print stream, where the key/value pairs are 
		/// written to.</param>
		/// <remarks>
		/// This is mainly useful for debugging purposes.
		/// </remarks>
		/// <exception cref="InvalidCastException">
		/// If this property contains a key or a value that isn't a string.
		/// </exception>
		public void List(Stream output) {
			StreamWriter writer = new StreamWriter(output);
			List(writer);
		}

		public void List(StreamWriter output) {
			output.WriteLine("-- listing properties --");

			foreach (var entry in this) {
				output.Write((String)entry.Key + "=");

				String s = (String)entry.Value;
				if (s != null && s.Length > 40)
					output.WriteLine(s.Substring(0, 37) + "...");
				else
					output.WriteLine(s);
			}
			output.Flush();
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

		/*
		TODO:
		public void storeToXML(Stream os, String comment) {
			storeToXML(os, comment, "UTF-8");
		}

		public void storeToXML(Stream os, String comment, String encoding) {
			if (os == null)
				throw new ArgumentNullException("os");
			if (encoding == null)
				throw new ArgumentNullException("encoding");
			try {
				DOMImplementationRegistry registry =
				  DOMImplementationRegistry.newInstance();
				DOMImplementation domImpl = registry.getDOMImplementation("LS 3.0");
				DocumentType doctype =
				  domImpl.createDocumentType("properties", null,
								 "http://java.sun.com/dtd/properties.dtd");
				Document doc = domImpl.createDocument(null, "properties", doctype);
				Element root = doc.getDocumentElement();
				if (comment != null) {
					Element commentElement = doc.createElement("comment");
					commentElement.appendChild(doc.createTextNode(comment));
					root.appendChild(commentElement);
				}
				Iterator iterator = entrySet().iterator();
				while (iterator.hasNext()) {
					Map.Entry entry = (Map.Entry)iterator.next();
					Element entryElement = doc.createElement("entry");
					entryElement.setAttribute("key", (String)entry.getKey());
					entryElement.appendChild(doc.createTextNode((String)
										entry.getValue()));
					root.appendChild(entryElement);
				}
				DOMImplementationLS loadAndSave = (DOMImplementationLS)domImpl;
				LSSerializer serializer = loadAndSave.createLSSerializer();
				LSOutput output = loadAndSave.createLSOutput();
				output.setByteStream(os);
				output.setEncoding(encoding);
				serializer.write(doc, output);
			} catch (ClassNotFoundException e) {
				throw (IOException)
				  new IOException("The XML classes could not be found.").initCause(e);
			} catch (InstantiationException e) {
				throw (IOException)
				  new IOException("The XML classes could not be instantiated.")
				  .initCause(e);
			} catch (IllegalAccessException e) {
				throw (IOException)
				  new IOException("The XML classes could not be accessed.")
				  .initCause(e);
			}
		}

		public void loadFromXML(InputStream input) {
			if (input == null)
				throw new NullPointerException("Null input stream supplied.");
			try {
				XMLInputFactory factory = XMLInputFactory.newInstance();
				// Don't resolve external entity references
				factory.setProperty("javax.xml.stream.isSupportingExternalEntities",
									Boolean.FALSE);
				XMLStreamReader reader = factory.createXMLStreamReader(input);
				String name, key = null;
				StringBuffer buf = null;
				while (reader.hasNext()) {
					switch (reader.next()) {
						case XMLStreamConstants.START_ELEMENT:
							name = reader.getLocalName();
							if (buf == null && "entry".equals(name)) {
								key = reader.getAttributeValue(null, "key");
								if (key == null) {
									String msg = "missing 'key' attribute";
									throw new InvalidPropertiesFormatException(msg);
								}
								buf = new StringBuffer();
							} else if (!"properties".equals(name) && !"comment".equals(name)) {
								String msg = "unexpected element name '" + name + "'";
								throw new InvalidPropertiesFormatException(msg);
							}
							break;
						case XMLStreamConstants.END_ELEMENT:
							name = reader.getLocalName();
							if (buf != null && "entry".equals(name)) {
								put(key, buf.toString());
								buf = null;
							} else if (!"properties".equals(name) && !"comment".equals(name)) {
								String msg = "unexpected element name '" + name + "'";
								throw new InvalidPropertiesFormatException(msg);
							}
							break;
						case XMLStreamConstants.CHARACTERS:
						case XMLStreamConstants.SPACE:
						case XMLStreamConstants.CDATA:
							if (buf != null)
								buf.append(reader.getText());
							break;
					}
				}
				reader.close();
			} catch (XMLStreamException e) {
				throw (InvalidPropertiesFormatException)
				  new InvalidPropertiesFormatException("Error in parsing XML.").
				  initCause(e);
			}
		}
		*/
	} // class Properties
}