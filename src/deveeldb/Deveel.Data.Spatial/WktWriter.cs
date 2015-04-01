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
using System.Globalization;
using System.IO;
using System.Text;

namespace Deveel.Data.Spatial {
	public sealed class WktWriter : IGeometryWriter {
		private readonly int dimensions;

		private const string MaxPrecisionFormat = "{0:R}";
		private NumberFormatInfo formatter;
		private string format;
		private string indentTabStr = "  ";

		public WktWriter() 
			: this(2) {
		}

		public WktWriter(int dimensions) {
			if (dimensions < 2 || dimensions > 3)
				throw new ArgumentException();

			this.dimensions = dimensions;
		}

		GeometryFormat IGeometryWriter.Format {
			get { return GeometryFormat.WellKnownText; }
		}

		object IGeometryWriter.Write(IGeometry geometry) {
			return Write(geometry);
		}

		public bool UseFormatting { get; set; }

		public bool UseMaxPrecision { get; set; }

		public int MaxCoordinatesPerLine { get; set; }

		private static NumberFormatInfo CreateFormatter(IPrecisionModel precisionModel) {
			var digits = precisionModel.MaximumSignificantDigits;
			var decimalPlaces = System.Math.Max(0, digits); // negative values not allowed

			// specify decimal separator explicitly to avoid problems in other locales
			var nfi = new NumberFormatInfo {
				NumberDecimalSeparator = ".",
				NumberDecimalDigits = decimalPlaces,
				NumberGroupSeparator = String.Empty,
				NumberGroupSizes = new int[] { }
			};
			return nfi;
		}

		public static string StringOfChar(char ch, int count) {
			var buf = new StringBuilder();
			for (var i = 0; i < count; i++)
				buf.Append(ch);

			return buf.ToString();
		}

		public string Write(IGeometry geometry) {
			var sb = new StringBuilder();

			using (var writer = new StringWriter(sb)) {
				WriteFormatted(geometry, writer);
			}

			return sb.ToString();
		}

		private void WriteFormatted(IGeometry geometry, TextWriter writer) {
			var precisionModel = geometry.Factory.PrecisionModel;
			UseMaxPrecision = precisionModel.PrecisionModelType == PrecisionModelType.Floating;

			formatter = CreateFormatter(geometry.PrecisionModel);
			format = "0." + StringOfChar('#', formatter.NumberDecimalDigits);
			WriteGeometry(geometry, 0, writer);

			UseMaxPrecision = false;
		}

		private void WriteGeometry(IGeometry geometry, int level, TextWriter writer) {
			Indent(level, writer);

			if (geometry is IPoint) {
				var point = (IPoint)geometry;
				WritePoint(point.Coordinate, level, writer);
			} else if (geometry is ILinearRing)
				WriteLinearRing((ILinearRing)geometry, level, writer);
			else if (geometry is ILineString)
				WriteLineString((ILineString)geometry, level, writer);
			else if (geometry is IPolygon)
				WritePolygon((IPolygon)geometry, level, writer);
			else if (geometry is IMultiPoint)
				WriteMultiPoint((IMultiPoint)geometry, level, writer);
			else if (geometry is IMultiLineString)
				WriteMultiLineString((IMultiLineString)geometry, level, writer);
			else if (geometry is IMultiPolygon)
				WriteMultiPolygon((IMultiPolygon)geometry, level, writer);
			else if (geometry is IGeometryCollection)
				WriteGeometryCollection((IGeometryCollection)geometry, level, writer);
			else
				throw new ArgumentException(String.Format("The geometry type {0} is not supported.", geometry.GetType()));
		}

		private void WritePoint(ICoordinate coordinate, int level, TextWriter writer) {
			writer.Write("POINT ");
			WritePointText(coordinate, level, writer);
		}

		private void WriteLineString(ILineString lineString, int level, TextWriter writer) {
			writer.Write("LINESTRING ");
			WriteLineStringText(lineString, level, false, writer);
		}

		private void WriteLinearRing(ILinearRing linearRing, int level, TextWriter writer) {
			writer.Write("LINEARRING ");
			WriteLineStringText(linearRing, level, false, writer);
		}

		private void WritePolygon(IPolygon polygon, int level, TextWriter writer) {
			writer.Write("POLYGON ");
			WritePolygonText(polygon, level, false, writer);
		}

		private void WriteMultiPoint(IMultiPoint multipoint, int level, TextWriter writer) {
			writer.Write("MULTIPOINT ");
			WriteMultiPointText(multipoint, level, writer);
		}

		private void WriteMultiLineString(IMultiLineString multiLineString, int level, TextWriter writer) {
			writer.Write("MULTILINESTRING ");
			WriteMultiLineStringText(multiLineString, level, false, writer);
		}

		private void WriteMultiPolygon(IMultiPolygon multiPolygon, int level, TextWriter writer) {
			writer.Write("MULTIPOLYGON ");
			WriteMultiPolygonText(multiPolygon, level, writer);
		}

		private void WriteGeometryCollection(IGeometryCollection geometryCollection, int level, TextWriter writer) {
			writer.Write("GEOMETRYCOLLECTION ");
			WriteGeometryCollectionText(geometryCollection, level, writer);
		}

		private void WritePointText(ICoordinate coordinate, int level, TextWriter writer) {
			if (coordinate == null) {
				writer.Write("EMPTY");
			} else {
				writer.Write("(");
				WriteCoordinate(coordinate, writer);
				writer.Write(")");
			}
		}

		private void WriteCoordinate(ICoordinate coordinate, TextWriter writer) {
			writer.Write(ToString(coordinate.X) + " " + ToString(coordinate.Y));

			if (dimensions >= 3 && !double.IsNaN(coordinate.Z)) {
				writer.Write(" " + ToString(coordinate.Z));
			}
		}

		private string ToString(double d) {
			var standard = d.ToString(format, formatter);
			if (!UseMaxPrecision) {
				return standard;
			}
			try {
				var converted = Convert.ToDouble(standard, formatter);
				// check if some precision is lost during text conversion: if so, use {0:R} formatter
				if (converted == d)
					return standard;
				return String.Format(formatter, MaxPrecisionFormat, d);
			} catch (OverflowException) {
				// Use MaxPrecisionFormat anyway
				return String.Format(formatter, MaxPrecisionFormat, d);
			}
		}

		private void WriteLineStringText(ILineString lineString, int level, bool doIndent, TextWriter writer) {
			if (lineString.IsEmpty)
				writer.Write("EMPTY");
			else {
				if (doIndent) Indent(level, writer);
				writer.Write("(");

				for (var i = 0; i < lineString.NumPoints; i++) {
					WriteCoordinate(lineString.GetCoordinate(i), writer);

					if (i < lineString.NumPoints - 1) {
						writer.Write(", ");
						if (MaxCoordinatesPerLine > 0 &&
							i % MaxCoordinatesPerLine == 0) {
							Indent(level + 1, writer);
						}						
					}
				}

				writer.Write(")");
			}
		}

		private void WritePolygonText(IPolygon polygon, int level, bool indentFirst, TextWriter writer) {
			if (polygon.IsEmpty)
				writer.Write("EMPTY");
			else {
				if (indentFirst) Indent(level, writer);
				writer.Write("(");
				WriteLineStringText(polygon.ExteriorRing, level, false, writer);

				for (var i = 0; i < polygon.NumInteriorRings; i++) {
					writer.Write(", ");

					WriteLineStringText(polygon.GetInteriorRing(i), level + 1, true, writer);
				}

				writer.Write(")");
			}
		}

		private void WriteMultiPointText(IMultiPoint multiPoint, int level, TextWriter writer) {
			if (multiPoint.IsEmpty)
				writer.Write("EMPTY");
			else {
				writer.Write("(");
				for (var i = 0; i < multiPoint.NumGeometries; i++) {
					writer.Write("(");
					WriteCoordinate((multiPoint.GetGeometry(i)).Coordinate, writer);
					writer.Write(")");

					if (i < multiPoint.NumGeometries - 1) {
						writer.Write(", ");
						IndentCoords(i, level + 1, writer);
					}
				}
				writer.Write(")");
			}
		}

		private void WriteMultiLineStringText(IMultiLineString multiLineString, int level, bool indentFirst, TextWriter writer) {
			if (multiLineString.IsEmpty)
				writer.Write("EMPTY");
			else {
				var level2 = level;
				var doIndent = indentFirst;
				writer.Write("(");
				for (var i = 0; i < multiLineString.NumGeometries; i++) {
					if (i > 0) {
						writer.Write(", ");
						level2 = level + 1;
						doIndent = true;
					}

					WriteLineStringText((ILineString)multiLineString.GetGeometry(i), level2, doIndent, writer);
				}
				writer.Write(")");
			}
		}

		private void WriteMultiPolygonText(IMultiPolygon multiPolygon, int level, TextWriter writer) {
			if (multiPolygon.IsEmpty)
				writer.Write("EMPTY");
			else {
				var level2 = level;
				var doIndent = false;
				writer.Write("(");
				for (var i = 0; i < multiPolygon.NumGeometries; i++) {
					if (i > 0) {
						writer.Write(", ");
						level2 = level + 1;
						doIndent = true;
					}
					WritePolygonText((IPolygon)multiPolygon.GetGeometry(i), level2, doIndent, writer);
				}
				writer.Write(")");
			}
		}

		private void WriteGeometryCollectionText(IGeometryCollection geometryCollection, int level, TextWriter writer) {
			if (geometryCollection.IsEmpty)
				writer.Write("EMPTY");
			else {
				var level2 = level;
				writer.Write("(");
				for (var i = 0; i < geometryCollection.NumGeometries; i++) {
					if (i > 0) {
						writer.Write(", ");
						level2 = level + 1;
					}
					WriteGeometry(geometryCollection.GetGeometry(i), level2, writer);
				}
				writer.Write(")");
			}
		}

		private void IndentCoords(int coordIndex, int level, TextWriter writer) {
			if (MaxCoordinatesPerLine <= 0 || coordIndex % MaxCoordinatesPerLine != 0)
				return;

			Indent(level, writer);
		}

		private void Indent(int level, TextWriter writer) {
			if (!UseFormatting || level <= 0) return;
			writer.Write("\n");
			for (int i = 0; i < level; i++)
				writer.Write(indentTabStr);
		}
	}
}
