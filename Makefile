SPARK_VERSION = 3.0.1
KAMU_VERSION = 0.0.1
ENGINE_VERSION = 0.9.1
IMAGE_REPO = kamudata
IMAGE_SPARK_UBER_TAG = $(SPARK_VERSION)_$(KAMU_VERSION)


.PHONY: image
image:
	docker build \
		--build-arg BASE_IMAGE=$(IMAGE_REPO)/spark-py-uber:$(IMAGE_SPARK_UBER_TAG) \
		-t $(IMAGE_REPO)/engine-spark:$(ENGINE_VERSION) \
		-f image/Dockerfile \
		.


.PHONY: image-push
image-push:
	docker push $(IMAGE_REPO)/engine-spark:$(ENGINE_VERSION)
