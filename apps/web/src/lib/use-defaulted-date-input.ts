import { useEffect, useState } from "react";

type DefaultedDateInputState = {
  value: string;
  onBlur: () => void;
  onChange: (nextValue: string) => void;
};

export function useDefaultedDateInput(defaultValue: string): DefaultedDateInputState {
  const [value, setValue] = useState(defaultValue);
  const [hasCustomValue, setHasCustomValue] = useState(false);

  useEffect(() => {
    if (value === defaultValue) {
      setHasCustomValue(false);
    }
  }, [defaultValue, value]);

  useEffect(() => {
    if (hasCustomValue) return;
    if (value === defaultValue) return;
    setValue(defaultValue);
  }, [defaultValue, hasCustomValue, value]);

  const onChange = (nextValue: string) => {
    setValue(nextValue);
    if (!nextValue) {
      setHasCustomValue(Boolean(defaultValue));
      return;
    }
    setHasCustomValue(nextValue !== defaultValue);
  };

  const onBlur = () => {
    if (value || !defaultValue) return;
    setValue(defaultValue);
    setHasCustomValue(false);
  };

  return { value, onBlur, onChange };
}
