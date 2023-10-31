select * 
from 
    customer_trusted as c
    join accelerometer_landing  as a on c.email = a.user;