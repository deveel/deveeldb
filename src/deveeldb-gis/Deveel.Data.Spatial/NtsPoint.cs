using System;

using NetTopologySuite.Geometries;

namespace Deveel.Data.Spatial {
	class NtsPoint : NtsGeometry, IPoint {
		public NtsPoint(NtsGeometryFactory factory, Point geometry) 
			: base(factory, geometry) {
			Point = geometry;
		}

		public Point Point { get; private set; }

		public double X {
			get { return Point.X; }
		}

		public double Y {
			get { return Point.Y; }
		}

		public double Z {
			get { return Point.Z; }
		}

		public double M {
			get { return Point.M; }
		}
	}
}
