using System;
using System.Collections.Generic;

using NetTopologySuite.Geometries;

namespace Deveel.Data.Spatial {
	public sealed class NtsGeometryFactory : IGeometryFactory {
		private readonly GeometryFactory factory;

		public NtsGeometryFactory(NtsPrecisionModel precisionModel, int srid) {
			factory = new GeometryFactory(precisionModel.WrappedPrecisionModel, srid);
			Srid = srid;
			PrecisionModel = precisionModel;
		}

		public int Srid { get; private set; }

		public IPrecisionModel PrecisionModel { get; private set; }

		public ICoordinateFactory CoordinateFactory { get; private set; }

		public IGeometry BuildGeometry(IEnumerable<IGeometry> geomList) {
			throw new NotImplementedException();
		}

		public IGeometry CreateGeometry(IGeometry g) {
			throw new NotImplementedException();
		}

		public IPoint CreatePoint(ICoordinate coordinate) {
			throw new NotImplementedException();
		}

		public ILineString CreateLineString(ICoordinate[] coordinates) {
			throw new NotImplementedException();
		}

		public ILinearRing CreateLinearRing(ICoordinate[] coordinates) {
			throw new NotImplementedException();
		}

		public IPolygon CreatePolygon(ILinearRing shell, ILinearRing[] holes) {
			throw new NotImplementedException();
		}

		public IPolygon CreatePolygon(ICoordinate[] coordinates) {
			throw new NotImplementedException();
		}

		public IPolygon CreatePolygon(ILinearRing shell) {
			throw new NotImplementedException();
		}

		public IMultiPoint CreateMultiPoint(ICoordinate[] coordinates) {
			throw new NotImplementedException();
		}

		public IMultiPoint CreateMultiPoint(IPoint[] point) {
			throw new NotImplementedException();
		}

		public IMultiLineString CreateMultiLineString(ILineString[] lineStrings) {
			throw new NotImplementedException();
		}

		public IMultiPolygon CreateMultiPolygon(IPolygon[] polygons) {
			throw new NotImplementedException();
		}

		public IGeometryCollection CreateGeometryCollection(IGeometry[] geometries) {
			throw new NotImplementedException();
		}

		public IGeometry ToGeometry(IEnvelope envelopeInternal) {
			throw new NotImplementedException();
		}
	}
}
