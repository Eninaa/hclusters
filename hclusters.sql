
DECLARE
		sourceArr geometry[];
		intersecsArr geometry[];
		countArr int[];
		resArr geometry[];
		resArr2 geometry[];

		arr geometry[];
		res geometry;
		size int;
		count int;
		intersec geometry;
		centr geometry;
		errStatus int;
		temp geometry;
		
		averageArea float;
BEGIN
	errStatus := 0;
	count := 0;
	averageArea := 0;
	size := cardinality(strings);
	/*инициализация массива исходной геометрий*/
	FOR i IN 1..size LOOP
    sourceArr := array_append(sourceArr, ST_MakeValid(ST_GeomFromGeoJSON(strings[i])));
	END LOOP;
	
	if (centroidNum > 0) then
	centr := ST_Centroid(sourceArr[centroidNum]);
	FOR i IN 1..size LOOP
		temp := ST_Translate(sourceArr[i], ST_X(centr) - ST_X(ST_Centroid(sourceArr[i])), ST_Y(centr) - ST_Y(ST_Centroid(sourceArr[i])));
    	sourceArr[i] := temp;
	END LOOP;
	end if;
	
	-- разбиение мультиполигонов на полигоны
	FOR i IN 1..size LOOP
		temp := sourceArr[i]; -- разделение массива resArr на отдельные мультиполигоны теперь sourceArr
		FOR j IN 1..ST_NumGeometries(temp) LOOP -- разбиение мультиполигона на полигоны 
			arr := array_append(arr, ST_GeometryN(temp, j));
		END LOOP;
	END LOOP;
	sourceArr := arr;
	size := cardinality(sourceArr);
	
	-- нахождение средней площади
	FOR i in 1..size LOOP
		averageArea = averageArea + ST_Area(sourceArr[i]);
	END LOOP;
	averageArea = averageArea/size;
	
	
	-- нахождение пересечений
	
	FOR i IN 1..size-1 LOOP
		FOR j IN i+1..size LOOP
		intersec := ST_Intersection(sourceArr[i], sourceArr[j]);
		intersecsArr := array_append(intersecsArr, intersec);
		END LOOP;
	END LOOP;
	
	-- проверка на вхождение кусков в начальные полигоны
	
	if cardinality(intersecsArr) is not NULL then
	FOR i IN 1..cardinality(intersecsArr) LOOP
		FOR j IN 1..size LOOP
		if ST_Within(ST_PointOnSurface(intersecsArr[i]), sourceArr[j]) THEN
			count := count + 1;
		end if;
	END LOOP;
			if count > (cardinality(sourceArr)/4) then
			-- если площадь куска больше 1^2м и больше чем средняя площадь/2
				if (ST_Area(intersecsArr[i]) > 8.1e-11 AND ST_Area(intersecsArr[i]) > averageArea/2) then
					resArr2 := array_append(resArr2, intersecsArr[i]);
				end if;
			end if;
			count := 0;

	END LOOP;
		res := ST_Union(resArr2);
		/*if (ST_GeometryType(res) = 'ST_MultiPolygon') then
			res := ST_ConcaveHull(res, 0.3, false);
		end if;*/

	end if;
	--geometry collection + удаление мелких объектов
errStatus := cardinality(intersecsArr);
res := ST_Intersection(res, ST_Buffer(res, 0,'join=mitre'));
res := ST_Simplifyvw(res, 0.0000000000001);

	RETURN query select ST_AsGeoJson(res), errStatus;
    END;
