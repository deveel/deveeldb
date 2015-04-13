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
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Deveel.Data.Util;

namespace Deveel.Data.Spatial {
	public sealed class WktReader : IGeometryReader {
		private readonly IPrecisionModel precisionModel;
		private readonly IGeometryFactory factory;
		private readonly ICoordinateFactory coordinateFactory;
		private StreamTokenizer tokenizer;

		private const string Empty = "EMPTY";
		private const string Comma = ",";
		private const string LParen = "(";
		private const string RParen = ")";

		public WktReader(IGeometryFactory factory) {
			this.factory = factory;
			coordinateFactory = factory.CoordinateFactory;
			precisionModel = factory.PrecisionModel;

			Srid = factory.Srid;
		}

		public int Srid { get; set; }

		GeometryFormat IGeometryReader.Format {
			get { return GeometryFormat.WellKnownText; }
		}

		IGeometry IGeometryReader.Read(object input) {
			if (input is TextReader)
				return Read((TextReader) input);
			if (input is Stream)
				return Read((Stream) input);
			if (input is string)
				return Read((string) input);

			throw new ArgumentException();
		}

		public IGeometry Read(string input) {
			using (var reader = new StringReader(input)) {
				return Read(reader);
			}
		}

		public IGeometry Read(Stream inputStream) {
			using (var streamReader = new StreamReader(inputStream)) {
				return Read(streamReader);
			}
		}

		public IGeometry Read(TextReader reader) {
			tokenizer = new StreamTokenizer(reader);
			// set tokenizer to NOT parse numbers
			tokenizer.ResetSyntax();
			tokenizer.SetWordChars('a', 'z');
			tokenizer.SetWordChars('A', 'Z');
			tokenizer.SetWordChars(128 + 32, 255);
			tokenizer.SetWordChars('0', '9');
			tokenizer.SetWordChars('-', '-');
			tokenizer.SetWordChars('+', '+');
			tokenizer.SetWordChars('.', '.');
			tokenizer.SetWhitespaceChars(0, ' ');
			tokenizer.SetCommentChar('#');

			try {
				return ReadGeometry();
			} catch (IOException e) {
				throw new FormatException("The input text is not in a valid format.");
			}
		}

		private string GetNextWord() {
			int type = tokenizer.NextToken();
			switch (type) {
				case (int) StreamTokenizer.TokenType.Word:
					String word = tokenizer.SVal;
					if (word.Equals(Empty, StringComparison.OrdinalIgnoreCase))
						return Empty;
					return word;

				case '(':
					return LParen;
				case ')':
					return RParen;
				case ',':
					return Comma;
			}

			ReadError("word");
			return null;
		}

		private bool IsNumberNext() {
			int type = tokenizer.NextToken();
			tokenizer.PushBackToken();
			return type == (int) StreamTokenizer.TokenType.Word;
		}

		private double GetNextNumber() {
			int type = tokenizer.NextToken();
			if (type == (int) StreamTokenizer.TokenType.Word) {
				double num;
				if (!Double.TryParse(tokenizer.SVal, out num))
					throw new FormatException(String.Format("The string '{0}' is not a valid number.", tokenizer.SVal));

				return num;
			}

			ReadError("number");

			return 0.0;
		}

		private String GetNextEmptyOrOpener() {
			String nextWord = GetNextWord();
			if (nextWord.Equals(Empty, StringComparison.OrdinalIgnoreCase) || 
				nextWord.Equals(LParen, StringComparison.OrdinalIgnoreCase)) {
				return nextWord;
			}

			ReadError(String.Format("{0} or {1}", Empty, LParen));

			return null;
		}

		private string GetNextCloserOrComma() {
			string nextWord = GetNextWord();
			if (nextWord.Equals(Comma) || 
				nextWord.Equals(RParen)) {
				return nextWord;
			}

			ReadError(String.Format("{0} or {1}", Comma, RParen));

			return null;
		}

		private String GetNextCloser() {
			String nextWord = GetNextWord();
			if (nextWord.Equals(RParen)) {
				return nextWord;
			}

			ReadError(RParen);
			return null;
		}

		private void ReadError(String expected) {
			if (tokenizer.Type == (int)StreamTokenizer.TokenType.Number)
				throw new FormatException("Unexpected NUMBER token");
			if (tokenizer.Type == (int) StreamTokenizer.TokenType.EOL)
				throw new FormatException("Unexpected EOL token");

			var tokenStr = TokenString();
			throw new FormatException(String.Format("Expected {0} but found {1}", expected, tokenStr));
		}

		private String TokenString() {
			switch (tokenizer.Type) {
				case (int)StreamTokenizer.TokenType.Number:
					return "<NUMBER>";
				case (int)StreamTokenizer.TokenType.EOL:
					return "End-of-Line";
				case (int)StreamTokenizer.TokenType.EOF: 
					return "End-of-Stream";
				case (int)StreamTokenizer.TokenType.Word: 
					return "'" + tokenizer.SVal + "'";
			}

			return "'" + (char)tokenizer.Type + "'";
		}

		private IGeometry ReadGeometry() {
			String type;

			try {
				type = GetNextWord();
			} catch (IOException) {
				return null;
			} catch (FormatException) {
				return null;
			}

			if (type.Equals("POINT", StringComparison.OrdinalIgnoreCase))
				return ReadPoint();
			if (type.Equals("LINESTRING", StringComparison.OrdinalIgnoreCase))
				return ReadLineString();
			if (type.Equals("LINEARRING", StringComparison.OrdinalIgnoreCase))
				return ReadLinearRing();
			if (type.Equals("POLYGON", StringComparison.OrdinalIgnoreCase))
				return ReadPolygon();
			if (type.Equals("MULTIPOINT", StringComparison.OrdinalIgnoreCase))
				return ReadMultiPoint();
			if (type.Equals("MULTILINESTRING", StringComparison.OrdinalIgnoreCase))
				return ReadMultiLineString();
			if (type.Equals("MULTIPOLYGON", StringComparison.OrdinalIgnoreCase))
				return ReadMultiPolygon();
			if (type.Equals("GEOMETRYCOLLECTION", StringComparison.OrdinalIgnoreCase)) {
				return ReadGeometryCollection();
			}

			throw new FormatException(String.Format("Unknown geometry type {0} in input.", type));
		}

		private ICoordinate[] GetCoordinates() {
			String nextToken = GetNextEmptyOrOpener();
			if (nextToken.Equals(Empty)) {
				return new ICoordinate[] {};
			}

			var coordinates = new List<ICoordinate> {
				GetPreciseCoordinate()
			};

			nextToken = GetNextCloserOrComma();

			while (nextToken.Equals(Comma)) {
				coordinates.Add(GetPreciseCoordinate());
				nextToken = GetNextCloserOrComma();
			}

			return coordinates.ToArray();
		}

		private ICoordinate GetPreciseCoordinate() {
			var x = GetNextNumber();
			var y = GetNextNumber();
			double? z = null;
			if (IsNumberNext()) {
				z = GetNextNumber();
			}

			ICoordinate coord;
			if (z != null) {
				coord = coordinateFactory.CreateCoordinate(x, y, z.Value);
			} else {
				coord = coordinateFactory.CreateCoordinate(x, y);
			}

			return precisionModel.MakePrecise(coord);
		}

		private IPoint[] ToPoints(ICoordinate[] coordinates) {
			return coordinates.Select(t => factory.CreatePoint(t)).ToArray();
		}

		private IGeometryCollection ReadGeometryCollection() {
			var nextToken = GetNextEmptyOrOpener();
			if (nextToken.Equals(Empty, StringComparison.OrdinalIgnoreCase))
				return factory.CreateGeometryCollection(new IGeometry[0]);

			var geometrys = new List<IGeometry>();
			var geometry = ReadGeometry();
			geometrys.Add(geometry);

			nextToken = GetNextCloserOrComma();
			while (nextToken.Equals(Comma, StringComparison.OrdinalIgnoreCase)) {
				geometry = ReadGeometry();
				geometrys.Add(geometry);
				nextToken = GetNextCloserOrComma();
			}

			return factory.CreateGeometryCollection(geometrys.ToArray());
		}

		private IMultiPolygon ReadMultiPolygon() {
			var nextToken = GetNextEmptyOrOpener();
			if (nextToken.Equals(Empty, StringComparison.OrdinalIgnoreCase)) {
				return factory.CreateMultiPolygon(new IPolygon[0]);
			}

			var polygons = new List<IPolygon>();
			var polygon = ReadPolygon();
			polygons.Add(polygon);

			nextToken = GetNextCloserOrComma();
			while (nextToken.Equals(Comma, StringComparison.OrdinalIgnoreCase)) {
				polygon = ReadPolygon();
				polygons.Add(polygon);
				nextToken = GetNextCloserOrComma();
			}

			return factory.CreateMultiPolygon(polygons.ToArray());
		}

		private IMultiLineString ReadMultiLineString() {
			var nextToken = GetNextEmptyOrOpener();
			if (nextToken.Equals(Empty, StringComparison.OrdinalIgnoreCase)) {
				return factory.CreateMultiLineString(new ILineString[0]);
			}

			var lineStrings = new List<ILineString>();
			var lineString = ReadLineString();

			lineStrings.Add(lineString);

			nextToken = GetNextCloserOrComma();
			while (nextToken.Equals(Comma)) {
				lineString = ReadLineString();
				lineStrings.Add(lineString);
				nextToken = GetNextCloserOrComma();
			}

			return factory.CreateMultiLineString(lineStrings.ToArray());
		}

		private IMultiPoint ReadMultiPoint() {
			return factory.CreateMultiPoint(ToPoints(GetCoordinates()));
		}

		private IPolygon ReadPolygon() {
			String nextToken = GetNextEmptyOrOpener();
			if (nextToken.Equals(Empty, StringComparison.OrdinalIgnoreCase)) {
				return factory.CreatePolygon(factory.CreateLinearRing(new ICoordinate[0]), new ILinearRing[0]);
			}

			var holes = new List<ILinearRing>();
			var shell = ReadLinearRing();

			nextToken = GetNextCloserOrComma();
			while (nextToken.Equals(Comma)) {
				var hole = ReadLinearRing();
				holes.Add(hole);
				nextToken = GetNextCloserOrComma();
			}

			return factory.CreatePolygon(shell, holes.ToArray());
		}

		private ILinearRing ReadLinearRing() {
			return factory.CreateLinearRing(GetCoordinates());
		}

		private ILineString ReadLineString() {
			return factory.CreateLineString(GetCoordinates());
		}

		private IPoint ReadPoint() {
			var nextToken = GetNextEmptyOrOpener();
			if (nextToken.Equals(Empty, StringComparison.OrdinalIgnoreCase)) {
				return factory.CreatePoint((ICoordinate)null);
			}

			var coord = GetPreciseCoordinate();
			GetNextCloser();

			return factory.CreatePoint(coord);
		}
	}
}
