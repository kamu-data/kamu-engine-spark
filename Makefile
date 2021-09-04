SPARK_IMAGE_VERSION = 3.1.2
SPARK_IMAGE = bitnami/spark:$(SPARK_IMAGE_VERSION)
ENGINE_IMAGE_VERSION = 0.11.2-spark_$(SPARK_IMAGE_VERSION)
ENGINE_IMAGE = kamudata/engine-spark:$(ENGINE_IMAGE_VERSION)


.PHONY: image
image:
	docker build \
		--build-arg BASE_IMAGE=$(SPARK_IMAGE) \
		-t $(ENGINE_IMAGE) \
		-f image/Dockerfile \
		.


.PHONY: image-push
image-push:
	docker push $(ENGINE_IMAGE)


.PHONY: test-data
test-data:
	mkdir -p test-data
	cd test-data && wget -O zipcodes.zip https://data.cityofnewyork.us/api/views/i8iw-xf4u/files/YObIR0MbpUVA0EpQzZSq5x55FzKGM2ejSeahdvjqR20?filename=ZIP_CODE_040114.zip