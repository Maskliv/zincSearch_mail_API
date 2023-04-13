<template>
  <div
    :className="`${
      email.isSelected
        ? 'bg-gradient-to-br from-dark-200 to-dark-300 cursor-pointer rounded-3xl drop-shadow-2xl'
        : ''
    } flex flex-row mt-1 py-3 px-5 hover:bg-gradient-to-br from-dark-200 to-dark-300 cursor-pointer rounded-3xl drop-shadow-2xl`"
  >
    <div className="flex flex-col w-full">
      <span className="text-sm text-light-200 font-medium mt-2">
        {{ email.Subject }}
      </span>
      <div className="flex items-center mt-2">
        <div
          className="flex flex-col w-8/12 text-mid-xs text-light-400 font-medium mr-1"
        >
          <span className="truncate ...">from: {{ email.From }}</span>
          <p className="truncate ...">
            to: {{ email.To[0] }}
            {{
              email.To.length > 1
                ? "... " + (email.To.length - 1) + " more"
                : ""
            }}
          </p>
        </div>
        <span
          className="text-center text-clip text-light-400 bg-dark-400 text-xs font-medium px-2 rounded-lg w-5/12"
        >
          {{ dateToString(email.Date) }}
        </span>
      </div>
      <span className="clamp text-xs font-normal text-light-400 mt-2 w-full">{{
        email.Body
      }}</span>
    </div>
  </div>
</template>

<script lang="ts">
import { defineComponent } from "vue";
import { Email } from "./store";

export default defineComponent({
  name: "EmailCard",
  props: {
    email: { type: Object as () => Email, required: true },
  },
  methods: {
    dateToString(date: Date) {
      var dateString =
        date.getFullYear().toString().padStart(4, "0") +
        "/" +
        (date.getMonth() + 1).toString().padStart(2, "0") +
        "/" +
        date.getDate().toString().padStart(2, "0") +
        " " +
        date.getHours().toString().padStart(2, "0") +
        ":" +
        date.getMinutes().toString().padStart(2, "0");
      return dateString;
    },
  },
});
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
.clamp {
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
  text-align: justify;
}

.clamp-hor {
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: horizontal;
  overflow: hidden;
  text-align: justify;
}

.text-mid-xs {
  font-size: 0.7rem;
  line-height: 0.9rem;
}
</style>
