using System;

using Deveel.Data.Sql.Objects;

using NetTopologySuite.Geometries;

namespace Deveel.Data.Spatial {
	class NtsGeometry : IGeometry {
		private readonly Geometry geometry;

		public NtsGeometry(NtsGeometryFactory factory, Geometry geometry) {
			Factory = factory;
			this.geometry = geometry;
		}

		public int CompareTo(object obj) {
			throw new NotImplementedException();
		}

		public int CompareTo(ISqlObject other) {
			throw new NotImplementedException();
		}

		public bool IsNull {
			get { return geometry == null; }
		}

		public bool IsComparableTo(ISqlObject other) {
			throw new NotImplementedException();
		}

		public int Srid {
			get { return geometry.SRID; }
		}

		public IGeometryFactory Factory { get; private set; }

		public IPrecisionModel PrecisionModel { get; private set; }

		public double Area {
			get { return geometry.Area; }
		}

		public double Length {
			get { return geometry.Length; }
		}

		public int NumGeometries {
			get { return geometry.NumGeometries; }
		}

		public int NumPoints {
			get { return geometry.NumPoints; }
		}

		public IGeometry Boundary {
			get { throw new NotImplementedException(); }
		}

		public IPoint Centroid {
			get { return (IPoint) NtsGeometryConverter.Convert(Factory, geometry.Centroid); }
		}

		public ICoordinate Coordinate { get; private set; }

		public ICoordinate[] Coordinates { get; private set; }

		public IGeometry Envelope {
			get { return NtsGeometryConverter.Convert(Factory, geometry.Envelope); }
		}

		public IPoint InteriorPoint { get; private set; }

		public IPoint PointOnSurface { get; private set; }

		public bool IsEmpty {
			get { return geometry.IsEmpty; }
		}

		public bool IsRectangle {
			get { return geometry.IsRectangle; }
		}

		public bool IsSimple {
			get { return geometry.IsSimple; }
		}

		public bool IsValid {
			get { return geometry.IsValid; }
		}

		public IGeometry ConvexHull() {
			var result = geometry.ConvexHull();
			return NtsGeometryConverter.Convert(Factory, result);
		}

		public IGeometry Difference(IGeometry other) {
			var ntsOther = NtsGeometryConverter.Convert(Factory, other);
			var result = geometry.Difference(ntsOther);
			return NtsGeometryConverter.Convert(Factory, result);
		}

		public IGeometry SymmetricDifference(IGeometry other) {
			throw new NotImplementedException();
		}

		public IGeometry Buffer(double distance) {
			var result = geometry.Buffer(distance);
			return NtsGeometryConverter.Convert(Factory, result);
		}

		public IGeometry Buffer(double distance, int quadrantSegments) {
			throw new NotImplementedException();
		}

		public IGeometry Intersection(IGeometry other) {
			throw new NotImplementedException();
		}

		public IGeometry Union(IGeometry other) {
			throw new NotImplementedException();
		}

		public IGeometry Union() {
			throw new NotImplementedException();
		}

		public bool EqualsTopologically(IGeometry other) {
			throw new NotImplementedException();
		}

		public bool EqualsExact(IGeometry other) {
			throw new NotImplementedException();
		}

		public bool EqualsExact(IGeometry other, double tolerance) {
			throw new NotImplementedException();
		}

		public bool EqualsNormalized(IGeometry g) {
			throw new NotImplementedException();
		}

		public bool Within(IGeometry g) {
			throw new NotImplementedException();
		}

		public bool Contains(IGeometry g) {
			throw new NotImplementedException();
		}

		public bool IsWithinDistance(IGeometry geom, double distance) {
			throw new NotImplementedException();
		}

		public bool CoveredBy(IGeometry g) {
			throw new NotImplementedException();
		}

		public bool Covers(IGeometry g) {
			throw new NotImplementedException();
		}

		public bool Crosses(IGeometry g) {
			throw new NotImplementedException();
		}

		public bool Intersects(IGeometry g) {
			throw new NotImplementedException();
		}

		public bool Overlaps(IGeometry g) {
			throw new NotImplementedException();
		}

		public bool Relate(IGeometry g, string intersectionPattern) {
			throw new NotImplementedException();
		}

		public bool Touches(IGeometry g) {
			throw new NotImplementedException();
		}

		public bool Disjoint(IGeometry g) {
			throw new NotImplementedException();
		}

		public IGeometry Reverse() {
			throw new NotImplementedException();
		}

		public double Distance(IGeometry g) {
			throw new NotImplementedException();
		}
	}
}
