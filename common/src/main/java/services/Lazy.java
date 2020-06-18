package services;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class Lazy<A> {

    private final Supplier<A> sValue;

    private A value;

    private Lazy(Supplier<A> value) {
        this.sValue = value;
    }

    public A get() {
        // Note that the following code is not thread safe. Thread safety
        // is not implemented here to keep the code simple, but can be
        // added easily.
        if (value == null) {
            value = sValue.get();
        }
        return value;
    }

    public Optional<A> getOptional() {
        var value = this.get();

        if (value == null) {
            return Optional.empty();
        }

        return Optional.of(value);
    }

    public <B> Lazy<B> map(Function<A, B> f) {
        return new Lazy<>(() -> f.apply(this.get()));
    }

    public <B> Lazy<B> map(Function<A, B> f, B defaultValue) {
        return new Lazy<>(() -> {
            try {
                return f.apply(this.get());
            } catch (Exception e) {
                return defaultValue;
            }
        });
    }

    public <B> Lazy<Optional<B>> mapOption(Function<A, B> f) {
        return new Lazy<>(() -> {
            try {
                return Optional.of(f.apply(this.get()));
            } catch (Exception e) {
                return Optional.empty();
            }
        });
    }

    public <B> Lazy<B> flatMap(Function<A, Lazy<B>> f) {
        return new Lazy<>(() -> f.apply(get()).get());
    }

    public void forEach(Consumer<A> c) {
        c.accept(get());
    }

    public static <A> Lazy<A> of(Supplier<A> a) {
        return new Lazy<>(a);
    }

    public static <A> Lazy<A> of(A a) {
        return new Lazy<>(() -> a);
    }

}