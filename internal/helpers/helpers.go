/*
 * Copyright 2023. Konstantin Vasilev (burmuley@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package helpers

func ItemInSlice[T comparable](item T, slice []T) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}

	return false
}

func CopySliceElems[T any](source []T, target *[]any) {
	for _, v := range source {
		*target = append(*target, v)
	}
}
