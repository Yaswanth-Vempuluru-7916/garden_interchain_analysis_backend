export const formatTimestampToIST = (timestampSeconds: number | null): string | null => {
    if (!timestampSeconds) return null;
    const date = new Date(timestampSeconds * 1000);
    const istOffset = 5.5 * 60 * 60 * 1000;
    const istDate = new Date(date.getTime() + istOffset);
    const istString = istDate.toISOString().replace("T", " ").substring(0, 23) + "+05:30";
    return istString;
  };