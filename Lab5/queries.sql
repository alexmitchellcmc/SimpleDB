Select *
From Cars C, ToWork T
Where T.day = "2015-11-20" AND C.plate = T.car_plate AND car_plate NOT IN
(Select car_plate From  Cars C, FromWork F Where C.plate = F.car_plate AND F.day = "2015-11-20");
