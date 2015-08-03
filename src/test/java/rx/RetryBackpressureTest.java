package rx;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import rx.functions.Func1;
import rx.observables.AbstractOnSubscribe;
import rx.schedulers.Schedulers;

public class RetryBackpressureTest {

  @Test
  public void testBackpressurePredicateRetry() throws Throwable {
    runTestCase(this::createWithRetryPredicate);
  }

  @Test
  public void testBackpressureNormalRetry() throws Throwable {
    runTestCase(this::createWithNormalRetry);
  }

  private void runTestCase(Func1<Long, Observable<Long>> flatMapper) throws Throwable {
    Observable<Long> obs = Observable.create(AbstractOnSubscribe.create(
        (state) -> state.onNext(1L)
    ));

    AtomicLong counter = new AtomicLong(0);
    AtomicReference<Throwable> errorRef = new AtomicReference<>(null);
    Subscription sub = obs
        .flatMap(flatMapper)
        .subscribe(
            (l) -> counter.incrementAndGet(),
            errorRef::set
        );

    long start = System.currentTimeMillis();
    while (!sub.isUnsubscribed() && (System.currentTimeMillis() - start) < 60000) {
      Thread.sleep(500);
    }

    Throwable error = errorRef.get();
    if (error != null) {
      throw error;
    }
  }

  private Observable<Long> createWithRetryPredicate(Long value) {
    return Observable.<Long>create(
        AbstractOnSubscribe.create((state) ->
            state.onNext(2L))
    )
    .retry((Integer count, Throwable throwable) -> count < 3)
    .subscribeOn(Schedulers.computation());
  }

  private Observable<Long> createWithNormalRetry(Long value) {
    return Observable.<Long>create(
        AbstractOnSubscribe.create((state) ->
            state.onNext(2L))
    )
    .retry(3)
    .subscribeOn(Schedulers.computation());
  }
}
