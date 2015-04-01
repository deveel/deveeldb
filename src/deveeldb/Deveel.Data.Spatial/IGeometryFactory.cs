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

namespace Deveel.Data.Spatial {
	public interface IGeometryFactory {
		int Srid { get; }

		IPrecisionModel PrecisionModel { get; }

		ICoordinateFactory CoordinateFactory { get; }


		IGeometry BuildGeometry(IEnumerable<IGeometry> geomList);

		IGeometry CreateGeometry(IGeometry g);

		IPoint CreatePoint(ICoordinate coordinate);

		ILineString CreateLineString(ICoordinate[] coordinates);

		ILinearRing CreateLinearRing(ICoordinate[] coordinates);

		IPolygon CreatePolygon(ILinearRing shell, ILinearRing[] holes);

		IPolygon CreatePolygon(ICoordinate[] coordinates);

		IPolygon CreatePolygon(ILinearRing shell);

		IMultiPoint CreateMultiPoint(ICoordinate[] coordinates);

		IMultiPoint CreateMultiPoint(IPoint[] point);

		IMultiLineString CreateMultiLineString(ILineString[] lineStrings);

		IMultiPolygon CreateMultiPolygon(IPolygon[] polygons);

		IGeometryCollection CreateGeometryCollection(IGeometry[] geometries);

		IGeometry ToGeometry(IEnvelope envelopeInternal);
	}
}