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

using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Spatial {
	public interface IGeometry : ISqlObject {
		int Srid { get; }

		IGeometryFactory Factory { get; }

		IPrecisionModel PrecisionModel { get; }

		double Area { get; }

		double Length { get; }

		int NumGeometries { get; }

		int NumPoints { get; }

		IGeometry Boundary { get; }

		IPoint Centroid { get; }

		ICoordinate Coordinate { get; }

		ICoordinate[] Coordinates { get; }

		IGeometry Envelope { get; }

		IPoint InteriorPoint { get; }

		IPoint PointOnSurface { get; }

		bool IsEmpty { get; }

		bool IsRectangle { get; }

		bool IsSimple { get; }

		bool IsValid { get; }


		IGeometry ConvexHull();

		IGeometry Difference(IGeometry other);

		IGeometry SymmetricDifference(IGeometry other);

		IGeometry Buffer(double distance);

		IGeometry Buffer(double distance, int quadrantSegments);

		IGeometry Intersection(IGeometry other);

		IGeometry Union(IGeometry other);

		IGeometry Union();

		bool EqualsTopologically(IGeometry other);

		bool EqualsExact(IGeometry other);

		bool EqualsExact(IGeometry other, double tolerance);

		bool EqualsNormalized(IGeometry g);

		bool Within(IGeometry g);

		bool Contains(IGeometry g);

		bool IsWithinDistance(IGeometry geom, double distance);

		bool CoveredBy(IGeometry g);

		bool Covers(IGeometry g);

		bool Crosses(IGeometry g);

		bool Intersects(IGeometry g);

		bool Overlaps(IGeometry g);

		bool Relate(IGeometry g, string intersectionPattern);

		bool Touches(IGeometry g);

		bool Disjoint(IGeometry g);

		IGeometry Reverse();

		double Distance(IGeometry g);
	}
}
