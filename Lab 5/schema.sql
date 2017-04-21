Create Table Person(
  id INT,
  name VARCHAR(255),
  phone INT
  PRIMARY KEY (id)
);

-- 
Create Table Cars(
  driver_id INT,
  capacity INT,
  model VARCHAR(255),
  plate VARCHAR(255),
  FOREIGN KEY(driver_id) REFERENCES Person(id)
  PRIMARY KEY (plate)
);

Create Table Passengers(
  day DATE,
  ride_time DATETIME,
  car_plate VARCHAR(255),
  person_id INT,
  FOREIGN KEY(person_id) REFERENCES Person(id),
  FOREIGN KEY(car_plate) REFERENCES Cars(plate),
  FOREIGN KEY(day, ride_time, car_plate) REFERENCES ToWork(day, ride_time, car_plate),
  FOREIGN KEY(day, ride_time, car_plate) REFERENCES FromWork(day, ride_time, car_plate)
);

CREATE TABLE ToWork(
  day DATE,
  ride_time DATETIME,
  car_plate VARCHAR(255),
  FOREIGN KEY(car_plate) REFERENCES Cars(plate),
  PRIMARY KEY(day, ride_time)
);

CREATE TABLE FromWork(
  day DATE,
  ride_time DATETIME,
  car_plate VARCHAR(255),
  FOREIGN KEY(car_plate) REFERENCES Cars(plate),
  PRIMARY KEY(day, ride_time)
);
