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

namespace Deveel.Data.Spatial {
	public interface IEnvelope : IEquatable<IEnvelope>, IComparable<IEnvelope> {
		double Area { get; }

		double Width { get; }

		double Height { get; }

		double MaxX { get; }

		double MaxY { get; }

		double MinX { get; }

		double MinY { get; }

		ICoordinate Centre { get; }

		bool Contains(double x, double y);

		bool Contains(ICoordinate p);

		bool Contains(IEnvelope other);

		bool Covers(double x, double y);

		bool Covers(ICoordinate p);

		bool Covers(IEnvelope other);

		double Distance(IEnvelope env);

		IEnvelope Intersection(IEnvelope env);

		IEnvelope Union(IPoint point);

		IEnvelope Union(ICoordinate coord);

		IEnvelope Union(IEnvelope box);

		bool Intersects(ICoordinate p);

		bool Intersects(double x, double y);

		bool Intersects(IEnvelope other);

		bool IsNull { get; }

		bool Overlaps(IEnvelope other);

		bool Overlaps(ICoordinate p);

		bool Overlaps(double x, double y);
	}
}
