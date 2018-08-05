package java8;

import java8.pojo.Dish;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class Java8Demo {

    public static void main(String[] args) throws Exception {
        //流操作
//        streamTest();

        //流遍历
//        foreach();

//        match();


        //TODO:start 第六章        用流收集数据
        //归约和汇总
        collectors();
    }

    public static void collectors(){
        List<Dish> menu = new ArrayList<>();

        menu.add(new Dish("pork", false, 800, Dish.Type.MEAT));
        menu.add(new Dish("beef", false, 700, Dish.Type.MEAT));
        menu.add(new Dish("chicken", false, 400, Dish.Type.MEAT));
        menu.add(new Dish("french fries", true, 530, Dish.Type.OTHER));
        menu.add(new Dish("rice", true, 350, Dish.Type.OTHER));
        menu.add(new Dish("season fruit", true, 120, Dish.Type.OTHER));
        menu.add(new Dish("pizza", true, 550, Dish.Type.OTHER));
        menu.add(new Dish("prawns", true, 300, Dish.Type.FISH));
        menu.add(new Dish("salmon", false, 450, Dish.Type.FISH));
        menu.add(new Dish("salmon", false, 450, Dish.Type.FISH));

        Long collect1 = menu.stream().collect(Collectors.counting());
        //等于上面一种
        Long collect2 = menu.stream().count();
        System.out.println(collect1);

        Comparator<Dish> dishCaloriesComparator =
                Comparator.comparingInt(Dish::getCalories);
        menu.stream().max(dishCaloriesComparator);


        IntSummaryStatistics menuStatistics =
                menu.stream().collect(Collectors.summarizingInt(Dish::getCalories));
        //一次计算多个值
        // IntSummaryStatistics{count=10, sum=4650, min=120, average=465.000000, max=800}
        System.out.println(menuStatistics);

        //字符串连接
        String shortMenu = menu.stream().map(Dish::getName).collect(Collectors.joining(","));
        System.out.println(shortMenu);

        int totalCalories = menu.stream().map(Dish::getCalories).reduce(0, (i, j) -> i + j);

        //分组
        Map<Dish.Type, Long> typesCount = menu.stream().collect(
                Collectors.groupingBy(Dish::getType, Collectors.counting()));
        System.out.println(typesCount);

        //分区函数
        Map<Boolean, List<Dish>> partitionedMenu =
                menu.stream().collect(Collectors.partitioningBy(Dish::isVegetarian));
    }
    public static void match(){
        List<Dish> menu = new ArrayList<>();

        menu.add(new Dish("pork", false, 800, Dish.Type.MEAT));
        menu.add(new Dish("beef", false, 700, Dish.Type.MEAT));
        menu.add(new Dish("chicken", false, 400, Dish.Type.MEAT));
        menu.add(new Dish("french fries", true, 530, Dish.Type.OTHER));
        menu.add(new Dish("rice", true, 350, Dish.Type.OTHER));
        menu.add(new Dish("season fruit", true, 120, Dish.Type.OTHER));
        menu.add(new Dish("pizza", true, 550, Dish.Type.OTHER));
        menu.add(new Dish("prawns", true, 300, Dish.Type.FISH));
        menu.add(new Dish("salmon", false, 450, Dish.Type.FISH));
        menu.add(new Dish("salmon", false, 450, Dish.Type.FISH));

        //只要一个为 true ,则为 true
        boolean anyMatch = menu.stream().anyMatch(Dish::isVegetarian);
        System.out.println(String.format("anyMatch:%s",anyMatch));

        //所有为 true ,则为 true
        boolean allMatch = menu.stream().allMatch(Dish::isVegetarian);
        System.out.println(String.format("allMatch:%s",allMatch));

        //与 allMatch 为反义词 , 所有为 false ,则为 true
        boolean noneMatch = menu.stream().noneMatch(Dish::isVegetarian);
        System.out.println(String.format("noneMatch:%s",noneMatch));

        //返回流中任意一个元素 , 比 findFirst 限制少
        String name1 = menu.stream().filter(Dish::isVegetarian).findAny().get().getName();
        System.out.println(String.format("name1:%s",name1));

        //返回匹配中的第一个元素
        Optional<Dish> first = menu.stream().filter(Dish::isVegetarian).findFirst();
        //有匹配中的元素则返回
        first.ifPresent(dish -> System.out.println(String.format("name2:%s", dish)));

    }

    public static void foreach(){
        List<String> title = Arrays.asList("JAVA8","In","Action");
        Stream<String> s = title.stream();
        s.forEach(System.out::println);
    }


    public static void streamTest() throws InterruptedException {
        List<Dish> menu = new ArrayList<>();

        menu.add(new Dish("pork", false, 800, Dish.Type.MEAT));
        menu.add(new Dish("beef", false, 700, Dish.Type.MEAT));
        menu.add(new Dish("chicken", false, 400, Dish.Type.MEAT));
        menu.add(new Dish("french fries", true, 530, Dish.Type.OTHER));
        menu.add(new Dish("rice", true, 350, Dish.Type.OTHER));
        menu.add(new Dish("season fruit", true, 120, Dish.Type.OTHER));
        menu.add(new Dish("pizza", true, 550, Dish.Type.OTHER));
        menu.add(new Dish("prawns", false, 300, Dish.Type.FISH));
        menu.add(new Dish("salmon", false, 450, Dish.Type.FISH));
        menu.add(new Dish("salmon", false, 450, Dish.Type.FISH));

        menu.stream().filter(d -> d.getCalories() < 400)
                .sorted(Comparator.comparing(Dish::getCalories))
                .distinct()
                .map(Dish::getName).collect(toList()).forEach(System.out::println);

        Thread.sleep(100);
        System.out.println("----------------并行流操作----------------");

        menu.parallelStream().filter(d -> d.getCalories() < 0)
                .sorted(Comparator.comparing(Dish::getCalories))
                .limit(8)
                //跳过 2 个元素
                .skip(2)
                .map(Dish::getName).collect(toList()).forEach(System.out::println);


        int sum = menu.parallelStream().mapToInt(Dish::getCalories)
                //.average();
                //.max();
                //.min();
                .sum();

        //第一种方式

        Stream<int[]> pythagoreanTriples =
                IntStream.rangeClosed(1, 100).boxed()
                        .flatMap(a ->
                                IntStream.rangeClosed(a, 100)
                                        .filter(b -> Math.sqrt(a * a + b * b) % 1 == 0)
                                        .mapToObj(b ->
                                                new int[]{a, b, (int)Math.sqrt(a * a + b * b)})
                        );


        pythagoreanTriples.limit(5)
                .forEach(t ->
                        System.out.println(t[0] + ", " + t[1] + ", " + t[2]));

        //第二种方式
        Stream<double[]> pythagoreanTriples2 =
                IntStream.rangeClosed(1, 100).boxed()
                        .flatMap(a ->
                                IntStream.rangeClosed(a, 100)
                                        .mapToObj(
                                                b -> new double[]{a, b, Math.sqrt(a*a + b*b)})
                                        .filter(t -> t[2] % 1 == 0));
    }
}
