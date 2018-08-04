package java8;

import java8.pojo.Dish;

interface DishFactory<P extends Dish>{
    P create(int calories,String name);
}
