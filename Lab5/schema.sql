Create Table Person(
  p_id INT,
  name VARCHAR(255),
  phone INT,
  PRIMARY KEY (p_id)
);

Create Table Cars(
  driver_id INT,
  capacity INT,
  model VARCHAR(255),
  plate VARCHAR(255),
  FOREIGN KEY(driver_id) REFERENCES Person(p_id),
  FOREIGN KEY (plate) REFERENCES FromWork(car_plate),
  PRIMARY KEY (plate)
);

Create Table Passengers(
  day DATE,
  ride_time DATETIME,
  car_plate VARCHAR(255),
  passenger_id INT,
  FOREIGN KEY(passenger_id) REFERENCES Person(p_id),
  FOREIGN KEY(car_plate) REFERENCES Cars(plate),
  FOREIGN KEY(day, ride_time, car_plate) REFERENCES ToWork(day, ride_time, car_plate),
  FOREIGN KEY(day, ride_time, car_plate) REFERENCES FromWork(day, ride_time, car_plate)
);

CREATE TABLE ToWork(
  day DATE,
  ride_time DATETIME,
  car_plate VARCHAR(255),
  FOREIGN KEY(car_plate) REFERENCES Cars(plate),
  PRIMARY KEY(day, ride_time, car_plate)
);

CREATE TABLE FromWork(
  day DATE,
  ride_time DATETIME,
  car_plate VARCHAR(255),
  FOREIGN KEY(car_plate) REFERENCES Cars(plate),
  PRIMARY KEY(day, ride_time, car_plate)
);

Insert Into ToWork (day, ride_time, car_plate) values ("2015-11-20", "2015-11-20 10:00:00", '1');
Insert Into ToWork (day, ride_time, car_plate) values ("2015-11-20", "2015-11-20 10:00:00", '2');
Insert Into ToWork (day, ride_time, car_plate) values ("2015-11-20", "2015-11-20 10:00:00", '3');
Insert Into ToWork (day, ride_time, car_plate) values ("2015-11-20", "2015-11-20 10:00:00", '4');
Insert Into ToWork (day, ride_time, car_plate) values ("2015-11-20", "2015-11-20 10:00:00", '5');

Insert Into Cars (driver_id, capacity, model, plate) values (1, 5, "Toyota Prius", '1');
Insert Into Cars (driver_id, capacity, model, plate) values (4, 5, "Toyota Corolla", '2');
Insert Into Cars (driver_id, capacity, model, plate) values (3, 5, "Toyota Camry", '3');
Insert Into Cars (driver_id, capacity, model, plate) values (2, 5, "Toyota Avalon", '4');
Insert Into Cars (driver_id, capacity, model, plate) values (2, 5, "Toyota Avalon", '5');

Insert Into FromWork (day, ride_time, car_plate) values ("2015-11-20", "2015-11-20 10:00:00", '1');
Insert Into FromWork (day, ride_time, car_plate) values ("2015-11-20", "2015-11-20 10:00:00", '2');
Insert Into FromWork (day, ride_time, car_plate) values ("2015-11-20", "2015-11-20 10:00:00", '3');
Insert Into FromWork (day, ride_time, car_plate) values ("2015-11-20", "2015-11-20 10:00:00", '4');
